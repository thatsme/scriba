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

  describe "telemetry events" do
    @tag :integration
    test "the four v0.1 events fire with the documented measurement/metadata shapes" do
      # Attach BEFORE start_supervised so the projection's first events are
      # observed. Using attach_many for one handler ID covering all four
      # events — :telemetry.detach by id then removes them all.
      ref = make_ref()
      handler_id = {:telemetry_smoke, ref}

      :telemetry.attach_many(
        handler_id,
        [
          [:scriba, :projection, :event, :start],
          [:scriba, :projection, :event, :stop],
          [:scriba, :projection, :batch, :stop]
        ],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(5, streams: 2)
      name = "pipeline-telemetry-#{:erlang.unique_integer([:positive])}"

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

      # Wait for the batch to commit. Once apply_batch has returned :ok the
      # batch :stop telemetry has been emitted; the per-event events fired
      # earlier in handle_message.
      eventually(fn -> assert length(TestTarget.commits(agent)) == 5 end, 2_000)

      received = drain_telemetry(ref, 100)

      starts = for {[:scriba, :projection, :event, :start], m, md} <- received, do: {m, md}
      stops = for {[:scriba, :projection, :event, :stop], m, md} <- received, do: {m, md}
      batches = for {[:scriba, :projection, :batch, :stop], m, md} <- received, do: {m, md}

      assert length(starts) == 5,
             "expected 5 :event :start events, got #{length(starts)}\n" <>
               "received: #{inspect(received)}"

      assert length(stops) == 5,
             "expected 5 :event :stop events, got #{length(stops)}"

      assert batches != [],
             "expected at least one :batch :stop event, got none"

      # :event :start shape: monotonic_time + system_time measurements,
      # projection / event_type / stream_id / position metadata.
      [{start_meas, start_meta} | _] = starts
      assert is_integer(start_meas.system_time)
      assert is_integer(start_meas.monotonic_time)
      assert start_meta.projection == %{name: name, version: 1}
      assert start_meta.event_type == "test_event"
      assert is_binary(start_meta.stream_id)
      assert is_integer(start_meta.position)
      assert Map.has_key?(start_meta, :telemetry_span_context)

      # :event :stop shape: duration + monotonic_time measurements; same
      # metadata as :start (we pass the same metadata map for both).
      [{stop_meas, stop_meta} | _] = stops
      assert is_integer(stop_meas.duration)
      assert stop_meas.duration >= 0
      assert is_integer(stop_meas.monotonic_time)
      assert stop_meta.projection == %{name: name, version: 1}
      assert stop_meta.event_type == "test_event"
      assert is_binary(stop_meta.stream_id)
      assert is_integer(stop_meta.position)

      # :batch :stop shape: duration + batch_size measurements; projection
      # metadata only.
      [{batch_meas, batch_meta} | _] = batches
      assert is_integer(batch_meas.duration)
      assert batch_meas.duration >= 0
      assert is_integer(batch_meas.batch_size)
      assert batch_meas.batch_size > 0
      assert batch_meta.projection == %{name: name, version: 1}
    end

    @tag :integration
    test ":event :exception fires with kind/reason/stacktrace in metadata when handler raises" do
      ref = make_ref()
      handler_id = {:telemetry_exception_smoke, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :event, :exception],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(1)
      name = "pipeline-telemetry-exc-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.RaisingHandler,
        batch_size: 5,
        batch_timeout: 50
      ]

      # Broadway logs the message failure at :error level (the span re-raises,
      # Broadway's processor catches and routes the message to its default
      # handle_failed/2 with logging). Capture to keep test output clean.
      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :event, :exception], measurements,
                        metadata},
                       2_000

        # Measurements: duration + monotonic_time (span shape).
        assert is_integer(measurements.duration)
        assert is_integer(measurements.monotonic_time)

        # Metadata: kind/reason/stacktrace merged INTO start metadata by
        # :telemetry.span/3 — NOT in measurements. This is the
        # empirically-verified contract from deps/telemetry/src/telemetry.erl:384.
        assert metadata.kind == :error
        assert %RuntimeError{message: "boom"} = metadata.reason
        assert is_list(metadata.stacktrace)
        assert metadata.projection == %{name: name, version: 1}
        assert metadata.event_type == "test_event"
        refute Map.has_key?(measurements, :kind)
        refute Map.has_key?(measurements, :reason)
        refute Map.has_key?(measurements, :stacktrace)
      end)
    end
  end

  @doc false
  def forward_telemetry(event, measurements, metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, event, measurements, metadata})
  end

  defmodule RaisingHandler do
    @moduledoc false
    def handle(_data, _meta), do: raise("boom")
  end

  defp drain_telemetry(ref, timeout) do
    receive do
      {^ref, event, measurements, metadata} ->
        [{event, measurements, metadata} | drain_telemetry(ref, timeout)]
    after
      timeout -> []
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
