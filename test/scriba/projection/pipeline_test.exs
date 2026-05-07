defmodule Scriba.Projection.PipelineTest do
  # async: false — uses :erlang.trace_pattern, which is BEAM-global. Running
  # this concurrently with other tests that touch the same MFA pattern would
  # race on the call_count counter.
  use ExUnit.Case, async: false

  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test

  describe "source-side dedup" do
    @tag :integration
    test "redelivered events are skipped — Pipeline restart, source replays, no duplicates" do
      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(10, streams: 3)
      name = "pipeline-dedup-#{:erlang.unique_integer([:positive])}"

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

      # Phase 1 — initial pass: every event flows through the handler.
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)
      first_pass = TestTarget.commits(agent)
      initial_positions = Scriba.Position.stream_positions(name, 1)
      assert length(first_pass) == 10
      assert map_size(initial_positions) == 3

      # Phase 2 — arm a deterministic completion signal for the replay.
      # Broadway emits [:broadway, :processor, :message, :stop] exactly once
      # per message after handle_message returns. Counting 10 of these
      # post-restart proves the replay flowed end-to-end through the dedup
      # check, without sleep-based timing assumptions.
      ref = make_ref()
      handler_id = {:dedup_message_counter, ref}

      :telemetry.attach(
        handler_id,
        [:broadway, :processor, :message, :stop],
        &__MODULE__.forward_broadway_message/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      # Phase 3 — kill the Pipeline child, restart it. Coordinator and the
      # position cache survive (they live above and beside the Pipeline in
      # the rest_for_one tree). The new Pipeline brings up a fresh
      # Scriba.Test.Source which re-yields all 10 events. Using direct
      # Supervisor.terminate_child / restart_child rather than
      # Coordinator.pause/resume — those wrap the same primitives but
      # the names imply pause/resume semantics that aren't real until .
      [{sup_pid, _}] = Registry.lookup(Scriba.Registry, {:projection_supervisor, name, 1})
      :ok = Supervisor.terminate_child(sup_pid, Scriba.Projection.Pipeline)
      {:ok, _} = Supervisor.restart_child(sup_pid, Scriba.Projection.Pipeline)

      # Phase 4 — drain 10 broadway-message-stop events. Once all 10
      # replayed events have completed handle_message, dedup has had its
      # chance on every one of them.
      for _ <- 1..10 do
        assert_receive {^ref, :broadway_message_stop}, 5_000
      end

      # Phase 5 — dedup caught every replayed event:
      #  - Test target commit log unchanged (handle_message returned :skip
      #    for the redelivered events; Test.apply_batch filters :skip).
      #  - Per-stream cursors unchanged (handle_batch's stream_advances
      #    filter excludes :skip results, so cache_put isn't called for
      #    streams whose batch contained only redelivered events).
      assert TestTarget.commits(agent) == first_pass
      assert Scriba.Position.stream_positions(name, 1) == initial_positions
    end
  end

  @doc false
  def forward_broadway_message(_event, _measurements, _metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, :broadway_message_stop})
  end

  describe "cache update path" do
    @tag :integration
    test "Position.stream_positions reflects per-stream max after a successful run" do
      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(10, streams: 3)
      name = "pipeline-cache-#{:erlang.unique_integer([:positive])}"

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

      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      # The Pipeline calls Scriba.Position.cache_put/4 for every stream
      # in the batch's stream_advances after a successful target.apply_batch.
      # After all 10 events flow through, the cache should hold the per-stream
      # max position for each of the 3 streams from Test.Events.list.
      expected =
        events
        |> Enum.group_by(& &1.stream_id)
        |> Map.new(fn {sid, evts} -> {sid, evts |> Enum.map(& &1.position) |> Enum.max()} end)

      actual = Scriba.Position.stream_positions(name, 1)

      assert actual == expected,
             "cache cursors differ from expected per-stream max:\n" <>
               "  expected: #{inspect(expected)}\n" <>
               "  actual:   #{inspect(actual)}"
    end
  end

  describe "partitioner integration" do
    @tag :integration
    test "Scriba.Partitioner.partition/2 is on the Pipeline's runtime path" do
      # Arm BEAM call-count tracing on the partitioner. This is a structural
      # guarantee against the dead-code finding from the diagnostic phase
      # (Pipeline used to bypass this module via inline `:erlang.phash2`).
      # If anyone ever reverts the wiring in pipeline.ex, this test fails
      # loudly instead of the partitioner module silently going dead.
      :erlang.trace_pattern({Scriba.Partitioner, :partition, 2}, true, [:call_count])

      on_exit(fn ->
        :erlang.trace_pattern({Scriba.Partitioner, :partition, 2}, false, [:call_count])
      end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(10, streams: 3)
      name = "pipeline-partitioner-#{:erlang.unique_integer([:positive])}"

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

      # Wait for events to flow through.
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      {:call_count, count} =
        :erlang.trace_info({Scriba.Partitioner, :partition, 2}, :call_count)

      assert count >= 10,
             "Scriba.Partitioner.partition/2 was called #{count} times — expected " <>
               "at least 10 (one per event entering Broadway's partition_by). " <>
               "Zero typically means pipeline.ex has reverted to inline " <>
               ":erlang.phash2 and the partitioner module is dead code again."
    end
  end

  defp eventually(fun, timeout) do
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
