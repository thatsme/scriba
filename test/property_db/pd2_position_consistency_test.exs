defmodule Scriba.PropertyDb.Pd2PositionConsistencyTest do
  @moduledoc """
  PD2 — position consistency.

  For arbitrary event streams without crashes, after the projection
  processes all events: every row in the read model has a corresponding
  `scriba_positions` row whose cursor is ≥ that event's position.

  The Multi atomicity in `Scriba.Target.Ecto.apply_batch/5` is what
  guarantees this — if the read-model insert and the per-stream cursor
  upsert both commit, they commit together; if either fails, both roll
  back. PD2 exercises 200 random event streams against a real Postgres
  to flush out any path that could let the read model drift ahead of the
  cursor.

  Cursor regression in the other direction is also relevant — if the
  cursor were to be ahead of the actual applied events, source-side
  dedup would silently skip work. The assertion below catches that as
  well, since it requires `cursor >= position` for every read-model row.
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

  property "every read-model event's stream has scriba_positions cursor >= event.position" do
    check all events <- Generators.events_gen_pd(min: 10, max: 200), max_runs: 200 do
      name = "pd2-#{:erlang.unique_integer([:positive])}"
      scoped_events = H.scope_event_ids(events, name)

      opts = [
        name: name,
        version: 1,
        source: {Scriba.Test.Source, events: scoped_events},
        target: {Scriba.Target.Ecto, repo: Repo},
        parallelism: 4,
        handler: Scriba.Test.PropertyDb.Handler,
        batch_size: 5,
        batch_timeout: 50
      ]

      # Fresh ref + prefix per iteration: ref differentiates this iteration
      # from previous ones at the BEAM-message level; prefix gives the
      # wait_for_read_model loop a precise match so leftover signals from
      # earlier iterations can't false-trigger this one.
      ref = make_ref()
      handler_id = {:pd2_batches, ref}
      # Unique child_spec id per iteration. Without this, every iteration
      # tries to register a child under id `Scriba.Projection.Supervisor`
      # in ExUnit's tracker, and iteration 2 fails with `:already_started`
      # because ExUnit still has iteration 1's entry. The matching
      # stop_supervised(sup_id) call in the after block both stops the
      # process and removes the entry from the tracker.
      sup_id = {ProjSup, name}
      sup_spec = Supervisor.child_spec({ProjSup, opts}, id: sup_id)

      :telemetry.attach(
        handler_id,
        [:broadway, :batch_processor, :stop],
        &H.forward_batch_done/4,
        %{test_pid: self(), ref: ref, prefix: name}
      )

      try do
        start_supervised!(sup_spec)

        :ok = H.wait_for_read_model(ref, name, length(scoped_events), 10_000)

        # Both queries scoped to this iteration's projection name so the
        # accumulated rows from earlier iterations in this same sandbox
        # transaction don't bleed in.
        %{rows: model_rows} =
          Ecto.Adapters.SQL.query!(
            Repo,
            "SELECT event_id, stream_id, position " <>
              "FROM test_read_models WHERE event_id LIKE $1",
            [name <> "-%"]
          )

        %{rows: pos_rows} =
          Ecto.Adapters.SQL.query!(
            Repo,
            "SELECT stream_id, position " <>
              "FROM scriba_positions " <>
              "WHERE projection_name = $1 AND projection_version = $2",
            [name, 1]
          )

        cursor_by_stream = Map.new(pos_rows, fn [sid, pos] -> {sid, pos} end)

        # Sanity: every event landed in the read model.
        assert length(model_rows) == length(scoped_events),
               "read-model count mismatch: expected #{length(scoped_events)}, got #{length(model_rows)}"

        # PD2 itself: every read-model row's stream has a cursor >= the row's
        # position.
        for [event_id, stream_id, event_position] <- model_rows do
          cursor = Map.get(cursor_by_stream, stream_id)

          assert cursor != nil,
                 "stream #{stream_id} has read-model row #{event_id} but no scriba_positions row"

          assert cursor >= event_position,
                 "stream #{stream_id}: row #{event_id} at position #{event_position}, " <>
                   "but scriba_positions cursor is #{cursor} (cursor must be ≥ position)"
        end
      after
        # Detach first so any in-flight batches finishing during shutdown
        # don't queue more :batch_done messages into our mailbox.
        :telemetry.detach(handler_id)

        # stop_supervised/1 stops the projection tree AND removes the entry
        # from ExUnit's tracker atomically. Without it, the tracker would
        # accumulate one entry per iteration (200 by end of run); 200
        # supervision trees of ~8-10 processes each adds scheduler pressure,
        # quadratic telemetry-handler fan-out, and ETS cache bloat. The
        # `:not_found` return is harmless — happens if the supervisor
        # already died for unrelated reasons.
        _ = stop_supervised(sup_id)

        # ETS entries in the application-scoped position cache from this
        # iteration's projection persist (no per-projection cleanup hook on
        # graceful stop). That's fine: unique `name` per iteration prevents
        # collision and the leftover entries are bounded (one row per stream
        # per iteration ≈ 1000 rows over 200 iterations).
      end
    end
  end
end
