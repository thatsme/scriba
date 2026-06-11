defmodule Scriba.PropertyDb.Pd3CursorResumeTest do
  @moduledoc """
  PD3 — recovery resumes from committed position.

  Two-phase per iteration:

    1. Phase 1 runs a projection through all N generated events. After
       all are committed, the source's `acked_cursor` equals N, the
       Pipeline cache holds per-stream cursors equal to each stream's
       max position, and `scriba_positions` in Postgres has rows for
       each (name, version, stream_id) tuple. Phase 1 stops cleanly
       via `stop_supervised/1`.

    2. Phase 2 starts a NEW projection with the SAME name+version but
       configures the source with `:start_from: N` (the cursor we read
       in phase 1). Source-side filtering should drop every event ≤ N
       at `init/1`, leaving the source's queue empty. Pipeline never
       sees any events; no batches form; `test_read_models` and
       `scriba_positions` stay exactly as phase 1 left them.

  Property: `test_read_models` row count for this projection is
  unchanged across phase-2 startup, AND no
  `[:broadway, :batch_processor, :stop]` telemetry event fires during
  the phase-2 settle window. The telemetry refute is structural defense
  against a partial leak — even if a future refactor lets some events
  past the source filter, the absence of a batch commit (rather than
  just "row count happened to match in the brief observation window")
  is what we want to assert.

  ## What this verifies and what it doesn't

  This exercises the source's `:start_from` filter against real
  Postgres and a real Ecto target. It does NOT independently exercise
  the Pipeline's source-side dedup: with start_from
  filtering everything, dedup is never invoked. The two layers are
  defense in depth. The fast-suite integration test
  exercises dedup directly; PD3 here verifies the source-side filter
  works end-to-end.

  Iterations: 100 (half PD2's count because
  each iteration runs two projections in sequence).
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  @moduletag :property_db
  @moduletag timeout: 600_000

  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Test.Generators
  alias Scriba.Test.PropertyDbHelpers, as: H
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: H.setup_sandbox(ctx)

  property "phase 2 with :start_from cursor processes zero events; read model unchanged" do
    check all events <- Generators.events_gen_pd(min: 10, max: 200), max_runs: 100 do
      name = "pd3-#{:erlang.unique_integer([:positive])}"
      scoped_events = H.scope_event_ids(events, name)
      total = length(scoped_events)

      # ---- Phase 1: run projection through all events ----
      ref1 = make_ref()
      handler_id1 = {:pd3_phase1, ref1}
      sup_id1 = {ProjSup, name, :phase1}

      opts1 = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: scoped_events},
        target: {Scriba.Target.Ecto, repo: Repo},
        parallelism: 4,
        handler: Scriba.Test.PropertyDb.Handler,
        batch_size: 5,
        batch_timeout: 50
      ]

      :telemetry.attach(
        handler_id1,
        [:broadway, :batch_processor, :stop],
        &H.forward_batch_done/4,
        %{test_pid: self(), ref: ref1, prefix: name}
      )

      cursor =
        try do
          sup_spec1 = Supervisor.child_spec({ProjSup, opts1}, id: sup_id1)
          start_supervised!(sup_spec1)

          :ok = H.wait_for_read_model(ref1, name, total, 10_000)

          # Read the source's acked_cursor before stopping. By the time
          # wait_for_read_model returns, the Pipeline has committed all
          # batches AND Broadway has called Source.ack/3 for the
          # successful messages, advancing acked_cursor. The synchronous
          # GenStage.call is itself a barrier for any prior async
          # :advance_acked_cursor messages still in the source's mailbox.
          source_pid = H.lookup_source(name, 1)
          c = Scriba.Test.Source.acked_cursor(source_pid)

          # Sanity: cursor should match the total event count (all
          # positions 1..total acked).
          assert c == total,
                 "phase 1: expected acked_cursor=#{total}, got #{c} — phase 2 will resume from a stale cursor"

          c
        after
          :telemetry.detach(handler_id1)
          _ = stop_supervised(sup_id1)
        end

      rows_after_phase1 = H.read_model_count(name)

      assert rows_after_phase1 == total,
             "phase 1 read-model count mismatch: expected #{total}, got #{rows_after_phase1}"

      # ---- Phase 2: restart with :start_from cursor; expect silent run ----
      ref2 = make_ref()
      handler_id2 = {:pd3_phase2, ref2}
      sup_id2 = {ProjSup, name, :phase2}

      opts2 = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: scoped_events, start_from: cursor},
        target: {Scriba.Target.Ecto, repo: Repo},
        parallelism: 4,
        handler: Scriba.Test.PropertyDb.Handler,
        batch_size: 5,
        batch_timeout: 50
      ]

      :telemetry.attach(
        handler_id2,
        [:broadway, :batch_processor, :stop],
        &H.forward_batch_done/4,
        %{test_pid: self(), ref: ref2, prefix: name}
      )

      try do
        sup_spec2 = Supervisor.child_spec({ProjSup, opts2}, id: sup_id2)
        start_supervised!(sup_spec2)

        # By the time start_supervised! returns, the Pipeline (and its
        # Broadway tree, including the Test.Source producer) is up.
        # Source.init/1 has already filtered events by :start_from, so
        # the queue should be empty.
        source_pid = H.lookup_source(name, 1)
        pending = Scriba.Test.Source.pending_count(source_pid)

        assert pending == 0,
               "phase 2: expected source queue empty (:start_from filtered everything ≤ #{cursor}), got #{pending} events"

        # Structural protection: refute_receive over a 500ms window. If
        # the source filter had a leak (or a future refactor changes
        # semantics), some event could escape and be processed in flight
        # — the source-state queue check above wouldn't catch that
        # because the events would already have been demanded out. A
        # batch_processor:stop firing here proves a commit happened,
        # which is the precise failure mode we want to name. 500ms is
        # plenty for Broadway's startup + first demand cycle on this
        # hardware (PD2 batches commit in low-double-digit ms).
        refute_receive {^ref2, :batch_done, ^name},
                       500,
                       "PD3 violated: source filter let an event ≤ #{cursor} through; Pipeline committed a batch in phase 2"

        # Final assertion: row count unchanged. Together with the
        # refute_receive above, this confirms phase 2 didn't add or
        # mutate any rows.
        rows_after_phase2 = H.read_model_count(name)

        assert rows_after_phase2 == rows_after_phase1,
               "PD3 violated: phase 2 added #{rows_after_phase2 - rows_after_phase1} rows after cursor-resume " <>
                 "(expected 0). cursor=#{cursor}, total=#{total}"
      after
        :telemetry.detach(handler_id2)
        _ = stop_supervised(sup_id2)
      end
    end
  end
end
