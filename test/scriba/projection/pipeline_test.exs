defmodule Scriba.Projection.PipelineTest do
  # async: false — uses :erlang.trace_pattern, which is BEAM-global. Running
  # this concurrently with other tests that touch the same MFA pattern would
  # race on the call_count counter.
  use ExUnit.Case, async: false

  require Logger

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
      # the names imply pause/resume semantics that aren't real yet.
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
      # guarantee against a dead-code regression
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
        batch_timeout: 50,
        # retry: false — this test pins the :event :exception telemetry
        # shape, not retry behavior. Disabling retries keeps the test
        # fast (no 1.1s default backoff) and prevents the span from
        # emitting multiple :exception events for the same event.
        retry: false
      ]

      # The Pipeline's try/rescue around :telemetry.span/3
      # catches the re-raise BEFORE Broadway sees the message as failed.
      # No Broadway-level log is produced — but kept under CaptureLog
      # defensively in case future Broadway versions log handle_message
      # exits differently.
      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :event, :exception], measurements, metadata},
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

  describe "dead-letter routing" do
    @tag :integration
    test "regression: {:error, _} no longer poisons the batch — successes commit, cursor advances past failures, dead-letter recorded" do
      # The structural fix this test pins: previously, a single
      # {:error, _} handler return inserted an Ecto.Multi.run step that
      # returned {:error, _}, failing the whole transaction. No reads
      # committed; no cursors advanced. The fix partitions failures out of
      # the Multi and routes them to dead-letter atomically with the cursor
      # advance. This test asserts the post-fix invariants for a mixed batch.

      # Attach a dead-letter telemetry listener so we can confirm the §9.3
      # event fires once per failed event with the documented metadata.
      ref = make_ref()
      handler_id = {:dead_letter_telemetry, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :dead_letter],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})

      # 3 events on the SAME stream — middle one fails. Cursor must
      # advance past all three; first and third must commit; middle must
      # land in the dead-letter list.
      events = [
        %Scriba.Event{
          id: "good-1",
          stream_id: "stream-a",
          type: "good",
          data: %{n: 1},
          position: 1,
          occurred_at: DateTime.utc_now()
        },
        %Scriba.Event{
          id: "bad-1",
          stream_id: "stream-a",
          type: "bad",
          data: %{n: 2},
          position: 2,
          occurred_at: DateTime.utc_now()
        },
        %Scriba.Event{
          id: "good-2",
          stream_id: "stream-a",
          type: "good",
          data: %{n: 3},
          position: 3,
          occurred_at: DateTime.utc_now()
        }
      ]

      name = "pipeline-dl-regression-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.SelectiveErrorHandler,
        batch_size: 10,
        batch_timeout: 50,
        # retry: false — this test pins the dead-letter routing shape,
        # not retry behavior. Retries are exercised by the dedicated
        # tests in "retry policy".
        retry: false
      ]

      start_supervised!({ProjSup, opts})

      # Wait for both good events to commit. If the poison-batch bug
      # regressed, this would time out — none of the events would commit.
      eventually(fn -> assert length(TestTarget.commits(agent)) == 2 end, 2_000)

      # (a) Good events committed.
      commits = TestTarget.commits(agent)
      committed_ids = Enum.map(commits, fn {id, _sid, _pos} -> id end)
      assert "good-1" in committed_ids
      assert "good-2" in committed_ids
      refute "bad-1" in committed_ids

      # (b) Cursor advanced past ALL three events — including the dead-
      # lettered one. Per §9.2, dead-lettering advances the cursor so the
      # projection doesn't get stuck.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-a" => 3}

      # (c) Dead-letter row exists with the right shape.
      dead_letters = TestTarget.dead_letters(agent)
      assert length(dead_letters) == 1
      [dl] = dead_letters
      assert dl.event.id == "bad-1"
      assert dl.error == {:error, :nope}

      # (d) Telemetry: one [:scriba, :projection, :dead_letter] event with
      # the §9.3 metadata shape.
      assert_receive {^ref, [:scriba, :projection, :dead_letter], measurements, metadata}, 500
      assert is_integer(measurements.system_time)
      assert metadata.projection == %{name: name, version: 1}
      assert metadata.position == 2
      assert metadata.stream_id == "stream-a"
      assert metadata.event_type == "bad"
      assert metadata.error_kind == "error"

      # No second dead_letter event — only the one bad event in this run.
      refute_receive {^ref, [:scriba, :projection, :dead_letter], _, _}, 50
    end

    @tag :integration
    test "handler raise dead-letters with error_kind matching the exception struct name" do
      ref = make_ref()
      handler_id = {:raise_dl_telemetry, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :dead_letter],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(1)
      name = "pipeline-dl-raise-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.RaisingHandler,
        batch_size: 5,
        batch_timeout: 50,
        # retry: false — pin dead-letter routing for raises without
        # paying for the default 1.1s retry backoff.
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :dead_letter], _measurements, metadata},
                       2_000

        # Exception kind label is the exception module name
        # (matches DeadLetter.normalize_error/1's `exception.__struct__`).
        assert metadata.error_kind == "Elixir.RuntimeError"
        assert metadata.event_type == "test_event"
        assert metadata.projection == %{name: name, version: 1}
      end)

      # Dead-letter recorded in the Test target's in-memory list with the
      # internal :exception tag (3-tuple shape — exception struct + stacktrace).
      [dl] = TestTarget.dead_letters(agent)
      assert {:exception, %RuntimeError{message: "boom"}, stacktrace} = dl.error
      assert is_list(stacktrace)

      # Cursor advanced past the dead-lettered event.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-0" => 1}
    end

    # A handler returning a shape outside §4.2 used to fall through
    # success_shape?/1's blacklist into the Target, where Multi assembly
    # raised FunctionClauseError *outside* apply_batch/6's
    # `case repo.transaction(...)`. Broadway failed the whole batch, and once
    # the source correctly stopped acknowledging failed batches, that became
    # an unbreakable crash loop: redeliver, same bad return, crash, repeat —
    # no dead letter, no progress, on one malformed event.
    #
    # A malformed return is a per-event user bug and must dead-letter like
    # any other, so the projection keeps moving.
    @tag :integration
    test "a handler return outside the six shapes dead-letters instead of wedging" do
      ref = make_ref()
      handler_id = {:malformed_dl_telemetry, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :dead_letter],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(1)
      name = "pipeline-dl-malformed-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {__MODULE__.StrictTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.MalformedReturnHandler,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :dead_letter], _measurements, metadata},
                       2_000

        # Distinguishable from "handler failed" — the handler is wrong.
        assert metadata.error_kind == "invalid_return"
      end)

      # The offending return is preserved verbatim as the diagnostic.
      [dl] = TestTarget.dead_letters(agent)
      assert dl.error == {:test_recrd, :ok}

      # Cursor advanced — the projection is not wedged on the bad event.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-0" => 1}
    end

    # Two events in one batch naming a Multi operation identically used to
    # raise inside Repo.transaction/1 — lazily, so outside apply_batch/6's
    # case. Broadway failed the batch, the source refused to ack it, it was
    # redelivered, and the same pair collided again: a deterministic, silent,
    # permanent loop. Now the first claim wins and later claimants are
    # dead-lettered individually, so the batch still commits.
    @tag :integration
    test "duplicate Multi keys in one batch dead-letter the later event, not the batch" do
      ref = make_ref()
      handler_id = {:collision_dl_telemetry, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :dead_letter],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      # Two events on ONE stream so they land in the same batch.
      events = Test.Events.list(2, streams: 1)
      name = "pipeline-dl-collision-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.StaticMultiKeyHandler,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :dead_letter], _measurements, metadata},
                       2_000

        assert metadata.error_kind == "multi_key_collision"
      end)

      # Exactly one dead letter — the FIRST event kept its claim and committed.
      assert [dl] = TestTarget.dead_letters(agent)
      assert {:multi_key_collision, [:example_projection]} = dl.error
      assert length(TestTarget.commits(agent)) == 1

      # Cursor advanced past both — the projection is not wedged.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-0" => 2}
    end
  end

  describe "per-event fallback on commit failure" do
    # The regression this closes: killing the producer on commit failure (so
    # the subscription rewinds and replays) turned every DETERMINISTIC commit
    # failure into an infinite loop — redeliver, fail identically, crash,
    # repeat. A unique violation on one event took its whole batch with it,
    # forever.
    #
    # The escape is to re-apply the batch one transaction per event and let
    # Scriba.Failure classify each failure by SQLSTATE.

    defp pg_error(code, name) do
      %Postgrex.Error{postgres: %{pg_code: code, code: name, severity: "ERROR", message: "x"}}
    end

    @tag :integration
    test "an integrity failure dead-letters one event and lets the rest commit" do
      ref = make_ref()
      handler_id = {:fallback_dl, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :dead_letter],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({Scriba.Target.Test, []})
      events = Test.Events.list(3, streams: 1)
      name = "pipeline-fallback-integrity-#{:erlang.unique_integer([:positive])}"

      # Fails whenever event at position 2 is in the batch — including when it
      # is alone in the per-event pass. Deterministic and event-specific,
      # which is precisely what :integrity means.
      fail_when = fn evts ->
        if Enum.any?(evts, &(&1.position == 2)),
          do: pg_error("23505", :unique_violation)
      end

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {__MODULE__.FailingTarget, agent: agent, fail_when: fail_when},
        parallelism: 1,
        handler: Scriba.Test.Projection,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :dead_letter], _m, metadata}, 2_000
        assert metadata.position == 2
        assert metadata.error_kind == "commit:23505 (unique_violation)"
      end)

      # Events 1 and 3 committed; only the poison event was dead-lettered.
      committed = Scriba.Target.Test.commits(agent) |> Enum.map(&elem(&1, 2))
      assert 1 in committed
      assert 3 in committed
      refute 2 in committed

      # Cursor advanced past all three — the projection is not wedged.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-0" => 3}
    end

    @tag :integration
    test "a transient failure replays the batch whole and commits nothing" do
      agent = start_supervised!({Scriba.Target.Test, []})
      events = Test.Events.list(3, streams: 1)
      name = "pipeline-fallback-transient-#{:erlang.unique_integer([:positive])}"

      # Deadlock: nothing about any event is wrong. Dead-lettering here would
      # be data loss, so the batch must replay untouched.
      fail_when = fn _evts -> pg_error("40P01", :deadlock_detected) end

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {__MODULE__.FailingTarget, agent: agent, fail_when: fail_when},
        parallelism: 1,
        handler: Scriba.Test.Projection,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})
        Process.sleep(300)
      end)

      # Nothing committed, nothing dead-lettered, cursor unmoved. The events
      # are still owed, which is what replay depends on.
      assert Scriba.Target.Test.commits(agent) == []
      assert Scriba.Target.Test.dead_letters(agent) == []
      assert Scriba.Position.stream_positions(name, 1) == %{}
    end

    @tag :integration
    test "every event failing on integrity grounds halts instead of draining the stream" do
      # SQLSTATE says a failure is deterministic. It does not say how many
      # events share the defect. A NOT NULL added to a column the handler
      # never populates makes EVERY insert fail 23502 — each individually
      # dead-letterable. Without a blast-radius guard the fallback would
      # dead-letter the whole stream, advance the cursor to head, and leave
      # info/2 reporting a caught-up projection over an empty read model.
      ref = make_ref()
      handler_id = {:wipeout_halt, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :halted],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({Scriba.Target.Test, []})
      events = Test.Events.list(3, streams: 1)
      name = "pipeline-wipeout-#{:erlang.unique_integer([:positive])}"

      fail_when = fn _evts -> pg_error("23502", :not_null_violation) end

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {__MODULE__.FailingTarget, agent: agent, fail_when: fail_when},
        parallelism: 1,
        handler: Scriba.Test.Projection,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})
        assert_receive {^ref, [:scriba, :projection, :halted], _m, _metadata}, 2_000
      end)

      # Nothing drained: no dead letters, no cursor movement, no silent
      # "caught up over an empty read model".
      assert Scriba.Target.Test.dead_letters(agent) == []
      assert Scriba.Position.stream_positions(name, 1) == %{}
    end

    @tag :integration
    test "a structural failure halts loudly rather than looping or discarding" do
      ref = make_ref()
      handler_id = {:fallback_halt, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :halted],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({Scriba.Target.Test, []})
      events = Test.Events.list(3, streams: 1)
      name = "pipeline-fallback-structural-#{:erlang.unique_integer([:positive])}"

      # Handler deployed ahead of its migration. Fails on every event forever;
      # dead-lettering would destroy the batch over a fixable mistake.
      fail_when = fn _evts -> pg_error("42703", :undefined_column) end

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {__MODULE__.FailingTarget, agent: agent, fail_when: fail_when},
        parallelism: 1,
        handler: Scriba.Test.Projection,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      # Announced, not silent — the original sin was a stall nobody could see.
      # Asserted on telemetry rather than log text: halt_batch/3 also logs, but
      # it logs from a Broadway-internal batch processor, and CaptureLog filters
      # by process ancestry. The telemetry event is the machine-readable half
      # and the one an operator alerts on, so that is what is pinned here.
      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        assert_receive {^ref, [:scriba, :projection, :halted], _m, metadata}, 2_000
        assert metadata.failure == "42703 (undefined_column)"
        assert metadata.projection == %{name: name, version: 1}
      end)

      # Queryable, not just edge-triggered. Telemetry fires once, at the
      # instant of the halt; an operator who was not subscribed then would
      # otherwise see :running for a projection that will never move again.
      eventually(
        fn ->
          {:ok, info} = Scriba.info(name, 1)
          assert info.status == :halted
          assert %Postgrex.Error{postgres: %{pg_code: "42703"}} = info.halt_reason
        end,
        2_000
      )

      # Nothing discarded: no dead letters, no cursor movement.
      assert Scriba.Target.Test.dead_letters(agent) == []
      assert Scriba.Position.stream_positions(name, 1) == %{}
    end
  end

  describe "retry policy" do
    @tag :integration
    test "{:error, _} twice then success — exactly one commit, no dead-letter, 3 handler invocations" do
      stream = "retry-success-#{:erlang.unique_integer([:positive])}"
      agent_name = start_counting_handler(stream, fails_remaining: 2)

      agent = start_supervised!({TestTarget, []})

      event = %Scriba.Event{
        id: "retry-ok-1",
        stream_id: stream,
        type: "test",
        data: %{},
        position: 1,
        occurred_at: DateTime.utc_now()
      }

      name = "pipeline-retry-success-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: [event]},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.CountingRetryHandler,
        batch_size: 5,
        batch_timeout: 50,
        # Tiny backoff — keeps the test under 50ms instead of 1.1s default.
        retry: [max_attempts: 3, backoff: [10, 10]]
      ]

      start_supervised!({ProjSup, opts})

      eventually(fn -> assert length(TestTarget.commits(agent)) == 1 end, 2_000)

      # Handler invoked exactly 3 times (2 failed retries + 1 success).
      assert Agent.get(agent_name, & &1.calls) == 3

      # One commit, no dead-letter, cursor advanced.
      assert TestTarget.commits(agent) == [{"retry-ok-1", stream, 1}]
      assert TestTarget.dead_letters(agent) == []
      assert Scriba.Position.stream_positions(name, 1) == %{stream => 1}
    end

    @tag :integration
    test "handler raises 3 times — dead-letter with original error_kind, cursor advances, 3 invocations" do
      ref = make_ref()
      handler_id = {:retry_raise_telemetry, ref}

      :telemetry.attach_many(
        handler_id,
        [
          [:scriba, :projection, :event, :exception],
          [:scriba, :projection, :dead_letter]
        ],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(1)
      name = "pipeline-retry-raise-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: events},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.RaisingHandler,
        batch_size: 5,
        batch_timeout: 50,
        retry: [max_attempts: 3, backoff: [10, 10]]
      ]

      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({ProjSup, opts})

        # One dead-letter event after all 3 attempts exhaust.
        assert_receive {^ref, [:scriba, :projection, :dead_letter], _, dl_metadata}, 2_000

        # error_kind reflects the original exception, not a "retry_exhausted"
        # wrapper. The retry layer is transparent to the dead-letter path.
        assert dl_metadata.error_kind == "Elixir.RuntimeError"
      end)

      # Three :exception telemetry events fired — one per retry attempt.
      # Each attempt re-invokes :telemetry.span/3, which emits its own
      # :start/:exception pair. Operators counting :exception per event_id
      # can detect retry activity this way (the v0.1 visibility story for
      # retries, no dedicated :retry event).
      exceptions = drain_event(ref, [:scriba, :projection, :event, :exception], 100)
      assert length(exceptions) == 3

      # Exactly one dead-letter row, original exception type preserved.
      [dl] = TestTarget.dead_letters(agent)
      assert {:exception, %RuntimeError{message: "boom"}, stacktrace} = dl.error
      assert is_list(stacktrace)

      # Cursor advanced past the dead-lettered event.
      assert Scriba.Position.stream_positions(name, 1) == %{"stream-0" => 1}
    end

    @tag :integration
    test "retry: false — no retries, immediate dead-letter, handler called exactly once" do
      stream = "retry-disabled-#{:erlang.unique_integer([:positive])}"
      # fails_remaining is large; if retry: false truly bypasses the loop,
      # the handler is called once and the {:error, _} routes straight to
      # dead-letter. If retry sneaks back in, we'd see calls > 1.
      agent_name = start_counting_handler(stream, fails_remaining: 999)

      agent = start_supervised!({TestTarget, []})

      event = %Scriba.Event{
        id: "retry-off-1",
        stream_id: stream,
        type: "test",
        data: %{},
        position: 1,
        occurred_at: DateTime.utc_now()
      }

      name = "pipeline-retry-disabled-#{:erlang.unique_integer([:positive])}"

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: [event]},
        target: {TestTarget, agent: agent},
        parallelism: 1,
        handler: __MODULE__.CountingRetryHandler,
        batch_size: 5,
        batch_timeout: 50,
        retry: false
      ]

      start_supervised!({ProjSup, opts})

      # Dead-letter routing is the success signal here — the {:error, _}
      # gets there without any retries.
      eventually(fn -> assert length(TestTarget.dead_letters(agent)) == 1 end, 2_000)

      assert Agent.get(agent_name, & &1.calls) == 1
      assert TestTarget.commits(agent) == []
      assert Scriba.Position.stream_positions(name, 1) == %{stream => 1}
    end
  end

  describe "parse_retry_opts/1 (unit)" do
    alias Scriba.Projection.Pipeline

    test "false returns max_attempts: 1, empty backoff (no retries)" do
      assert Pipeline.parse_retry_opts(false) == %{max_attempts: 1, backoff: []}
    end

    test "nil returns the documented default (3 attempts, [100, 1000, 10_000] backoff)" do
      assert Pipeline.parse_retry_opts(nil) == %{
               max_attempts: 3,
               backoff: [100, 1000, 10_000]
             }
    end

    test "true returns the documented default" do
      assert Pipeline.parse_retry_opts(true) == %{
               max_attempts: 3,
               backoff: [100, 1000, 10_000]
             }
    end

    test "keyword list with valid max_attempts and backoff" do
      assert Pipeline.parse_retry_opts(max_attempts: 5, backoff: [10, 20, 30, 40]) ==
               %{max_attempts: 5, backoff: [10, 20, 30, 40]}
    end

    test "partial keyword list merges with defaults" do
      assert Pipeline.parse_retry_opts(max_attempts: 2) == %{
               max_attempts: 2,
               backoff: [100, 1000, 10_000]
             }
    end

    test "raises when backoff list is too short for max_attempts" do
      assert_raise ArgumentError, ~r/at least max_attempts - 1 = 4 entries/, fn ->
        Pipeline.parse_retry_opts(max_attempts: 5, backoff: [100, 1000])
      end
    end

    test "raises when max_attempts is not a positive integer" do
      assert_raise ArgumentError, ~r/positive integer/, fn ->
        Pipeline.parse_retry_opts(max_attempts: 0)
      end

      assert_raise ArgumentError, ~r/positive integer/, fn ->
        Pipeline.parse_retry_opts(max_attempts: :infinity)
      end
    end

    test "raises when backoff contains non-integers" do
      assert_raise ArgumentError, ~r/non-negative integers/, fn ->
        Pipeline.parse_retry_opts(backoff: [100, "bad", 10_000])
      end
    end

    test "raises when backoff contains negative integers" do
      assert_raise ArgumentError, ~r/non-negative integers/, fn ->
        Pipeline.parse_retry_opts(backoff: [100, -50, 10_000])
      end
    end
  end

  @doc false
  def forward_telemetry(event, measurements, metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, event, measurements, metadata})
  end

  defp drain_event(ref, target_event, timeout) do
    receive do
      {^ref, ^target_event, measurements, metadata} ->
        [{measurements, metadata} | drain_event(ref, target_event, timeout)]
    after
      timeout -> []
    end
  end

  defp start_counting_handler(stream_id, opts) do
    name = String.to_atom("scriba_test_retry_#{stream_id}")

    defaults = %{
      fails_remaining: 0,
      on_fail: {:error, :transient},
      on_succeed: {:test_record, :ok},
      calls: 0
    }

    state = Map.merge(defaults, Map.new(opts))

    {:ok, _pid} = Agent.start_link(fn -> state end, name: name)

    on_exit(fn ->
      case Process.whereis(name) do
        nil -> :ok
        pid -> Agent.stop(pid)
      end
    end)

    name
  end

  defmodule RaisingHandler do
    @moduledoc false
    def handle(_data, _meta), do: raise("boom")
  end

  defmodule MalformedReturnHandler do
    @moduledoc false
    # Not in StrictTarget's vocabulary — a plausible typo for {:test_record, _}.
    def handle(_data, _meta), do: {:test_recrd, :ok}
  end

  defmodule StaticMultiKeyHandler do
    @moduledoc false
    # Every event returns a Multi naming its operation the same way — the
    # commanded_ecto_projections habit, safe under one-transaction-per-event
    # and a collision once Scriba merges a batch.
    def handle(_data, _meta) do
      {:multi, Ecto.Multi.run(Ecto.Multi.new(), :example_projection, fn _r, _c -> {:ok, 1} end)}
    end
  end

  defmodule FailingTarget do
    @moduledoc """
    Wraps `Scriba.Target.Test` and fails commits according to an injected rule,
    so the per-event fallback can be exercised without Postgres.

    `:fail_when` is a 1-arity function over the batch's events returning
    `nil` (commit) or a reason (fail with it). Because the fallback re-invokes
    `apply_batch/6` with a single event, the same function decides both the
    batch attempt and each per-event attempt — which is exactly the
    distinction under test.
    """
    @behaviour Scriba.Target

    @impl Scriba.Target
    def init(opts) do
      {:ok, agent_state} = Scriba.Target.Test.init(opts)
      {:ok, Map.put(agent_state, :fail_when, Keyword.fetch!(opts, :fail_when))}
    end

    @impl Scriba.Target
    def apply_batch(events, results, projection, advances, dead_letters, state) do
      case state.fail_when.(events) do
        nil ->
          Scriba.Target.Test.apply_batch(
            events,
            results,
            projection,
            advances,
            dead_letters,
            state
          )

        reason ->
          {:error, reason, state}
      end
    end
  end

  defmodule StrictTarget do
    @moduledoc """
    Scriba.Target.Test's storage with a declared result vocabulary.

    Scriba.Target.Test deliberately exports no `valid_result?/1`, so it accepts
    any non-`:skip` result — which is what a test double wants, but means it
    cannot exercise the Pipeline's invalid-return path. This target has the
    same storage and a fixed vocabulary, standing in for Scriba.Target.Ecto
    (whose own path needs Postgres).
    """
    @behaviour Scriba.Target

    @impl Scriba.Target
    defdelegate init(opts), to: Scriba.Target.Test

    @impl Scriba.Target
    defdelegate apply_batch(events, results, projection, advances, dead_letters, state),
      to: Scriba.Target.Test

    @impl Scriba.Target
    def valid_result?(:skip), do: true
    def valid_result?({:test_record, _}), do: true
    def valid_result?(_other), do: false
  end

  defmodule SelectiveErrorHandler do
    @moduledoc false
    # Returns {:error, :nope} for events whose data has type "bad" (per the
    # event :type field, passed through to data.n by Test.Events shape).
    # Everything else returns a success-shape result so the Test target
    # records the commit.
    def handle(_data, %{type: "bad"}), do: {:error, :nope}
    def handle(_data, _meta), do: {:test_record, :ok}
  end

  defmodule CountingRetryHandler do
    @moduledoc false
    # Stateful handler used by the retry tests. Reads a counter
    # Agent registered under `:"scriba_test_retry_#{stream_id}"` whose state
    # is `%{fails_remaining: N, on_fail: result, on_succeed: result, calls: N}`.
    #
    # Each call increments `calls`. While `calls <= fails_remaining`, returns
    # `on_fail` (default `{:error, :transient}`). Once `calls > fails_remaining`,
    # returns `on_succeed` (default `{:test_record, :ok}`).
    #
    # Letting the test inspect `calls` after the projection settles makes
    # "how many times did retry actually fire" a direct assertion rather
    # than a flaky timing one.
    def handle(_data, %{stream_id: stream_id}) do
      agent = String.to_atom("scriba_test_retry_#{stream_id}")

      Agent.get_and_update(agent, fn state ->
        new_state = %{state | calls: state.calls + 1}

        if new_state.calls <= state.fails_remaining do
          {state.on_fail, new_state}
        else
          {state.on_succeed, new_state}
        end
      end)
    end
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
