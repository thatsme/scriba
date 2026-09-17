defmodule Scriba.Projection.CoordinatorTest do
  use ExUnit.Case, async: true

  alias Scriba.Projection.Coordinator
  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test

  setup do
    # Start Agent first → ProjSup second. ExUnit teardown stops in reverse,
    # so ProjSup (and its Pipeline's final batch) finishes before the Agent
    # is taken down — no "no process" log spam during teardown.
    agent = start_supervised!({TestTarget, []})
    events = Test.Events.list(10, streams: 3)
    name = "coord-#{:erlang.unique_integer([:positive])}"

    opts = [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {TestTarget, agent: agent},
      parallelism: 2,
      handler: Scriba.Test.Projection,
      batch_size: 5,
      batch_timeout: 50
    ]

    start_supervised!({ProjSup, opts})

    %{name: name, version: 1, agent: agent}
  end

  describe "state transitions" do
    test ":initializing auto-transitions to :running once Pipeline producer is registered", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
    end

    test ":running → :paused on pause", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused
    end

    test ":paused → :running on resume", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused

      assert :ok = Coordinator.resume(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
    end

    test ":running → :stopped on stop (passes through :draining)", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped
    end

    test ":paused → :stopped on stop", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped
    end
  end

  describe "invalid command/state combinations" do
    test "pause from :paused returns {:error, {:invalid_state, :paused}}", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert {:error, {:invalid_state, :paused}} = Coordinator.pause(name, v)
    end

    test "resume from :running returns {:error, {:invalid_state, :running}}", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert {:error, {:invalid_state, :running}} = Coordinator.resume(name, v)
    end
  end

  describe "end-to-end with Test source and Test target" do
    test "all events flow source → handler → target", %{name: name, version: v, agent: agent} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      assert TestTarget.safe_position(agent) >= 1
    end

    test "preserves per-stream ordering", %{name: name, version: v, agent: agent} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      commits = TestTarget.commits(agent)

      # commits is now [{event_id, stream_id, position}, ...] in commit order;
      # group by the stream_id field directly and assert per-stream order.
      by_stream = Enum.group_by(commits, fn {_id, sid, _pos} -> sid end)

      for {stream_id, stream_commits} <- by_stream do
        positions = Enum.map(stream_commits, fn {_id, _sid, p} -> p end)

        assert positions == Enum.sort(positions),
               "per-stream ordering violated for #{stream_id}: #{inspect(positions)}"
      end
    end
  end

  describe "halt/3 — structural failure is a queryable state" do
    # Halt used to be edge-triggered only: telemetry plus a log line at the
    # instant it happened, and nothing afterwards. Scriba.info/2 reported
    # :running for a projection that would never move again, so an operator
    # who was not subscribed at that moment had a stopped projection and no
    # way to see it. That is the same silent-stall shape the halt path exists
    # to replace, one level up.

    test "moves the projection to :halted and records the cause", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      reason = %Postgrex.Error{postgres: %{pg_code: "42703", code: :undefined_column}}
      :ok = Coordinator.halt(name, v, reason)

      eventually(fn -> assert Coordinator.state(name, v) == :halted end)

      {:ok, info} = Scriba.info(name, v)
      assert info.status == :halted
      assert info.halt_reason == reason
    end

    test "keeps the FIRST reason when reported repeatedly", %{name: name, version: v} do
      # A source that redelivers re-presents the same batch, so the Pipeline
      # can report more than once. The original cause is the useful one.
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      :ok = Coordinator.halt(name, v, :first)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end)
      :ok = Coordinator.halt(name, v, :second)

      eventually(fn ->
        {:ok, info} = Scriba.info(name, v)
        assert info.halt_reason == :first
      end)
    end

    test "pause and resume are rejected; stop is the way out", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.halt(name, v, :boom)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end)

      assert {:error, {:invalid_state, :halted}} = Scriba.pause(name, v)
      assert {:error, {:invalid_state, :halted}} = Scriba.resume(name, v)

      # Halting stops acknowledging; it does not tear the Pipeline down. Stop
      # is the operator's exit once the underlying cause is fixed.
      assert :ok = Scriba.stop(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :stopped end)
    end

    test "halt_reason is nil in every other state", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      {:ok, info} = Scriba.info(name, v)
      assert info.status == :running
      assert info.halt_reason == nil
    end
  end

  describe "pause/resume held-demand semantics" do
    @tag :integration
    test "pause halts new commits; resume drains remaining events", %{
      name: name,
      version: v,
      agent: agent
    } do
      # Wait for projection to be live.
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      # Attach :batch :stop telemetry — detecting "pipeline has quieted
      # after pause" via telemetry-absence is more deterministic than a
      # raw wall-clock sleep.
      ref = make_ref()
      attach_telemetry(ref, [[:scriba, :projection, :batch, :stop]], %{name: name, version: v})

      # Pause. Some number K (≤ 10) may commit before our pause signal
      # lands at the source — Broadway has already prefetched into
      # processors.
      assert :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused

      # Wait until 200ms has elapsed with no further :batch :stop telemetry.
      # That window covers any in-flight messages still working through
      # Broadway after the pause signal landed.
      settle_telemetry_quiet(ref, 200)

      commits_after_settle = length(TestTarget.commits(agent))

      # Snapshot must be stable — proves pause is actually holding the
      # source, not just slow.
      Process.sleep(50)

      assert length(TestTarget.commits(agent)) == commits_after_settle,
             "commit count grew during pause from #{commits_after_settle} to " <>
               "#{length(TestTarget.commits(agent))} — pause didn't hold the source"

      # Resume — source drains accumulated demand, remaining events flow.
      assert :ok = Coordinator.resume(name, v)
      assert Coordinator.state(name, v) == :running

      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)
    end

    @tag :integration
    test ":paused and :resumed telemetry events fire with projection metadata", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      ref = make_ref()

      attach_telemetry(
        ref,
        [
          [:scriba, :projection, :paused],
          [:scriba, :projection, :resumed]
        ],
        %{name: name, version: v}
      )

      :ok = Coordinator.pause(name, v)

      assert_receive {^ref, [:scriba, :projection, :paused], pm, pmeta}, 500
      assert is_integer(pm.system_time)
      assert pmeta.projection == %{name: name, version: v}

      :ok = Coordinator.resume(name, v)

      assert_receive {^ref, [:scriba, :projection, :resumed], rm, rmeta}, 500
      assert is_integer(rm.system_time)
      assert rmeta.projection == %{name: name, version: v}
    end

    @tag :integration
    test "pause then stop transitions :paused → :stopped directly", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused

      # The Pipeline is still alive in :paused. Stop must terminate it.
      assert [{pipeline_pid_before, _}] = Registry.lookup(Scriba.Registry, {:pipeline, name, v})
      assert Process.alive?(pipeline_pid_before)

      :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped

      # Pipeline process is terminated synchronously — Supervisor.terminate_child
      # waits for the exit. Registry's monitor-based entry cleanup is async
      # (handles a :DOWN message), so the entry may linger briefly. We assert
      # the actual-process-death directly and wait briefly for Registry to
      # catch up.
      refute Process.alive?(pipeline_pid_before)
      eventually(fn -> assert Registry.lookup(Scriba.Registry, {:pipeline, name, v}) == [] end)
    end
  end

  describe "Broadway producer naming convention smoke test" do
    @tag :integration
    test "the Pipeline registers its producer under {name, version, \"Producer_0\"}", %{
      name: name,
      version: v
    } do
      # If a Broadway upgrade changes the suffix from "Producer_0" to
      # something else, this test fails loudly — Coordinator.pause/resume
      # depends on this exact key shape via Pipeline.get_producer_pid/2.
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      lookup = Registry.lookup(Scriba.Internals.Registry, {name, v, "Producer_0"})

      assert [{pid, _}] = lookup, """
      Pipeline did not register its Broadway producer under \
      {name, version, "Producer_0"} in Scriba.Internals.Registry. \
      Coordinator.pause/resume depends on this exact key shape. \
      Check whether a Broadway upgrade changed the producer name suffix.

      Lookup result was: #{inspect(lookup)}
      """

      assert is_pid(pid)
      assert pid == Scriba.Projection.Pipeline.get_producer_pid(name, v)
    end
  end

  describe "resume drains pending demand correctly" do
    @tag :integration
    test "events queued in the source pre-pause flow through after resume in correct order",
         %{name: name, version: v, agent: agent} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      # All 10 events may flow before pause lands. Wait for completion,
      # then verify ordering — which is the invariant pause/resume must
      # not disturb. Per-stream ordering is the architectural contract
      # (§3 rule 2); pause/resume must not produce a state where events
      # are processed out of order on a stream.
      :ok = Coordinator.pause(name, v)
      :ok = Coordinator.resume(name, v)

      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      commits = TestTarget.commits(agent)
      by_stream = Enum.group_by(commits, fn {_id, sid, _pos} -> sid end)

      for {stream_id, stream_commits} <- by_stream do
        positions = Enum.map(stream_commits, fn {_id, _sid, p} -> p end)

        assert positions == Enum.sort(positions),
               "pause/resume broke per-stream order for #{stream_id}: #{inspect(positions)}"
      end
    end
  end

  describe "a Pipeline that goes DOWN while the projection is paused" do
    # A pause lives in the producer, and a replacement producer starts
    # unpaused, so the Coordinator has to reapply it. Without that, the
    # projection resumes processing while `Scriba.info/2` still says :paused —
    # and because the stale monitor ref matches no later DOWN, it is never
    # noticed again either.
    #
    # The DOWN is delivered directly rather than by killing the Pipeline.
    # `Process.exit(pipeline, :kill)` brings the projection's own supervisor
    # down with it, which restarts the Coordinator too and so tests something
    # else entirely. What this clause owns is the Coordinator's reaction to
    # losing the process it monitors; that is what is driven here.

    test "reapplies the pause to the producer", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      producer = Scriba.Projection.Pipeline.get_producer_pid(name, v)
      assert Scriba.Test.Source.paused?(producer)

      # Unpause it behind the Coordinator's back, which is the state a
      # replacement producer comes up in.
      :ok = Scriba.Test.Source.resume(producer)
      refute Scriba.Test.Source.paused?(producer)

      report_pipeline_down(name, v)

      eventually(fn -> assert Coordinator.state(name, v) == :paused end, 2_000)

      assert Scriba.Test.Source.paused?(producer),
             "the projection reports :paused but its producer is emitting"
    end

    test "resumes normally afterwards", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)

      report_pipeline_down(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :paused end, 2_000)

      assert :ok = Coordinator.resume(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
    end

    test "monitors the Pipeline again, so a later DOWN is still noticed", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)

      ref_before = pipeline_ref(name, v)
      report_pipeline_down(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :paused end, 2_000)

      # A fresh ref is the half that the state alone cannot show: keeping the
      # dead one would leave the projection paused forever beside an
      # unwatched Pipeline.
      ref_after = pipeline_ref(name, v)
      assert is_reference(ref_after)
      refute ref_after == ref_before

      # And it acts on it: a second DOWN is handled like the first.
      report_pipeline_down(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :paused end, 2_000)
    end
  end

  describe "a Pipeline that goes DOWN while the projection is halted" do
    test "stays halted, and keeps the cause", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)

      reason = %Postgrex.Error{postgres: %{pg_code: "42703", code: :undefined_column}}
      :ok = Coordinator.halt(name, v, reason)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end)

      report_pipeline_down(name, v)

      # The cause is a schema or a permission, so a replacement Pipeline meets
      # the same wall. Reporting :running until it does would be reporting
      # progress that is not happening.
      eventually(fn -> assert Coordinator.state(name, v) == :halted end, 2_000)

      {:ok, info} = Scriba.info(name, v)
      assert info.status == :halted
      assert info.halt_reason == reason
    end

    test "monitors the Pipeline again", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.halt(name, v, :boom)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end)

      ref_before = pipeline_ref(name, v)
      report_pipeline_down(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end, 2_000)

      ref_after = pipeline_ref(name, v)
      assert is_reference(ref_after)
      refute ref_after == ref_before
    end

    test "stop is still the way out afterwards", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.halt(name, v, :boom)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end)

      report_pipeline_down(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :halted end, 2_000)

      assert :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped
    end
  end

  ## Test helpers

  defp coordinator_pid(name, v) do
    assert [{pid, _}] = Registry.lookup(Scriba.Registry, {:coordinator, name, v})
    pid
  end

  defp pipeline_ref(name, v) do
    {_state, data} = :sys.get_state(coordinator_pid(name, v))
    data.pipeline_ref
  end

  # Tells the Coordinator its Pipeline died, without killing anything: the
  # message it would receive from its own monitor.
  defp report_pipeline_down(name, v) do
    coordinator = coordinator_pid(name, v)
    {_state, data} = :sys.get_state(coordinator)

    send(coordinator, {:DOWN, data.pipeline_ref, :process, data.pipeline_pid, :killed})

    :ok
  end

  # :telemetry handlers are node-global, not scoped to the attaching process
  # or test. Every projection alive on the node emits into this handler,
  # including those of other async tests running concurrently. Forwarding
  # unconditionally therefore delivered foreign events to the test's mailbox.
  #
  # That was not theoretical: settle_telemetry_quiet/2 waits for a window with
  # no :batch :stop, and the async ordering property keeps its own projections
  # committing for ~70s. The window never opened and the pause/resume test
  # timed out at 60s — reproducible with just those two files, no database
  # involved. The :paused/:resumed test had the same latent bug in a milder
  # form: assert_receive would match another projection's event and then fail
  # the metadata assertion against it.
  #
  # Matching `projection` in both the metadata and the handler config makes
  # the filter the head's job — a non-matching event falls through to the
  # catch-all clause below and is dropped.
  @doc false
  def forward_telemetry(event, measurements, %{projection: projection} = metadata, %{
        test_pid: pid,
        ref: ref,
        projection: projection
      }) do
    send(pid, {ref, event, measurements, metadata})
  end

  def forward_telemetry(_event, _measurements, _metadata, _config), do: :ok

  defp attach_telemetry(ref, event_names, projection) do
    handler_id = {:coordinator_test, ref}

    :telemetry.attach_many(
      handler_id,
      event_names,
      &__MODULE__.forward_telemetry/4,
      %{test_pid: self(), ref: ref, projection: projection}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  # Blocks until `quiet_ms` milliseconds have elapsed without a
  # [:scriba, :projection, :batch, :stop] event arriving. Used to detect
  # "the Pipeline has drained in-flight work" after a pause signal —
  # telemetry-absence is deterministic where a raw Process.sleep would
  # not be.
  defp settle_telemetry_quiet(ref, quiet_ms) do
    receive do
      {^ref, [:scriba, :projection, :batch, :stop], _, _} ->
        settle_telemetry_quiet(ref, quiet_ms)
    after
      quiet_ms -> :ok
    end
  end

  defp eventually(fun, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, 20)
  end

  defp do_eventually(fun, deadline, interval) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) > deadline do
        reraise e, __STACKTRACE__
      else
        Process.sleep(interval)
        do_eventually(fun, deadline, interval)
      end
  end
end
