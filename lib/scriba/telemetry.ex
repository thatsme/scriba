defmodule Scriba.Telemetry do
  @moduledoc """
  Catalog of telemetry events emitted by Scriba.

  Events are emitted directly from the modules where they fire — there is
  no central emitter. This module documents the surface so users attaching
  handlers (via `:telemetry.attach_many/4`) have a single place to read.

  ## v0.1 events

  | Event | Emitter | Measurements | Metadata |
  | --- | --- | --- | --- |
  | `[:scriba, :projection, :event, :start]` | `Scriba.Projection.Pipeline` (via `:telemetry.span/3`) | `monotonic_time`, `system_time` | `projection`, `event_type`, `stream_id`, `position`, `telemetry_span_context` |
  | `[:scriba, :projection, :event, :stop]` | same | `duration`, `monotonic_time` | same as `:start` |
  | `[:scriba, :projection, :event, :exception]` | same | `duration`, `monotonic_time` | start metadata + `kind`, `reason`, `stacktrace` |
  | `[:scriba, :projection, :event, :skipped]` | the Pipeline (no handler ran) | `system_time` | `projection`, `reason` (`:dedup` or `:handler`), `event_type`, `stream_id`, `position` |
  | `[:scriba, :projection, :batch, :stop]` | `Scriba.Projection.Pipeline.handle_batch/4` (manual emit, success branch only) | `duration`, `batch_size` | `projection` |
  | `[:scriba, :projection, :dead_letter]` | `Scriba.Projection.Pipeline.handle_batch/4` (one per dead-lettered event, after Multi commit) | `system_time` | `projection`, `position`, `stream_id`, `event_type`, `error_kind` |
  | `[:scriba, :projection, :started]` | `Scriba.Projection.Coordinator` (first `:initializing → :running`, once per process lifetime) | `system_time` | `projection` |
  | `[:scriba, :projection, :paused]` | `Scriba.Projection.Coordinator` (on `:running → :paused` transition, after source pause signal sent) | `system_time` | `projection` |
  | `[:scriba, :projection, :resumed]` | `Scriba.Projection.Coordinator` (on `:paused → :running` transition, after source resume signal sent) | `system_time` | `projection` |
  | `[:scriba, :projection, :cache_initialized]` | `Scriba.Position.init_cache/3` | `wiped_count`, `preloaded_count` | `name`, `version`, `source` |
  | `[:scriba, :source, :batch, :failed]` | the source's acknowledger (a batch failed to commit; nothing was acknowledged) | `count` | `subscription`, `reason` |
  | `[:scriba, :projection, :halted]` | the Pipeline (structural commit failure; the projection has stopped making progress) | `system_time` | `projection`, `reason`, `failure` (SQLSTATE label) |

  `projection` is `%{name: String.t(), version: pos_integer()}`. `event_type`
  is `event.type` (source-adapter-supplied). `duration` is monotonic-time
  units — pass through `System.convert_time_unit/3` to get ms/µs.

  Batch `:stop` is emitted only when the target's `apply_batch` returns
  `:ok` — i.e. when the Multi committed.

  Batch *failure* is observable through two events, and they mean different
  things:

    * `[:scriba, :source, :batch, :failed]` — a batch did not commit and
      nothing in it was acknowledged. Emitted by the source rather than the
      pipeline because that is where the consequence lands: the producer stops
      so the subscription rewinds and redelivers. Isolated occurrences are
      normal under transient database trouble. **A sustained stream of them
      means no progress.** See `Scriba.BatchCommitError`.

    * `[:scriba, :projection, :halted]` — a batch failed with a *structural*
      error (`Scriba.Failure` classifies SQLSTATE class 42 and anything
      unrecognised this way): the schema or permissions do not match the code.
      Neither replaying nor dead-lettering can resolve that, so the projection
      deliberately stops. **This one is a page, not a warning.** Nothing is
      lost — no event is acknowledged and no cursor moves — but nothing
      proceeds either until someone fixes the cause and restarts.

  Deterministic per-event failures do not reach either event. A commit failure
  classified as `:integrity` is isolated by the pipeline's per-event fallback
  and recorded in `scriba_dead_letters` with `error_kind` `"commit:<SQLSTATE>"`,
  and the projection carries on.

  ## Counting skips, and why it matters

  Skipped events emit no `:event :start`/`:stop` pair — no handler ran, so
  there is no latency to measure. They emit
  `[:scriba, :projection, :event, :skipped]` instead, with `reason`:

    * `:dedup` — the source redelivered an event at or below this stream's
      committed cursor. It was already applied.
    * `:handler` — the handler returned `:skip`. This projection does not
      care about this event.

  Skip is the **only** outcome that leaves no other trace: no read-model row,
  no dead-letter row, no cursor anomaly. Without this event the conservation
  identity

      events delivered == read-model rows + dead letters + skipped

  cannot be closed by anyone, including an operator trying to answer "has my
  projection silently skipped anything?" A projection that has skipped a
  thousand events and one that is genuinely caught up look identical from
  every other signal Scriba emits.

  A rising `:dedup` count is usually healthy (crash recovery replaying).
  A rising `:handler` count on event types you expected to handle is a
  missing `handle/2` clause.

  ## Out of scope for v0.1

  Lag and throughput events (`[:scriba, :projection, :lag]`,
  `[:scriba, :projection, :throughput]`) are v0.2 per
  `SCRIBA_ARCHITECTURE.md` §2. See
  §7.5 of the architecture doc for the implementation note on periodic
  timers that the v0.2 work will need.
  """
end
