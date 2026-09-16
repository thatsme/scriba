defmodule Scriba.Telemetry do
  @moduledoc """
  Catalog of telemetry events emitted by Scriba.

  Events are emitted directly from the modules where they fire — there is
  no central emitter. This module documents the surface so users attaching
  handlers (via `:telemetry.attach_many/4`) have a single place to read.

  ## Events

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
  | `[:scriba, :source, :standby]` | the source, on each failed subscribe attempt (another subscriber holds the name) | `attempt`, `retry_in_ms` | `subscription`, `reason` |
  | `[:scriba, :source, :subscribed]` | the source, when it acquires the subscription | `attempts` | `subscription` |
  | `[:scriba, :source, :batch, :failed]` | the source's acknowledger (a batch failed to commit; nothing was acknowledged) | `count` | `subscription`, `reason` |
  | `[:scriba, :projection, :lag]` | `Scriba.Projection.Coordinator`, on a timer (`:lag_interval`, default 5s; `0` disables) | `lag_ms`, `watermark` | `projection`, `status` |
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

    * `[:scriba, :projection, :halted]` — the projection has stopped making
      progress, for one of two reasons. Either a batch failed with a
      *structural* error (`Scriba.Failure` classifies SQLSTATE class 42 and
      anything unrecognised this way) — the schema or permissions do not
      match the code — or **every** attempted write in a batch failed on
      integrity grounds, which `Scriba.Circuit` reads as the same thing
      rather than draining the stream into dead letters. In the second case
      `failure` is `{:integrity_wipeout, n}` rather than a SQLSTATE label.
      Neither replaying nor dead-lettering can resolve either, so the
      projection deliberately stops. **This one is a page, not a warning.** Nothing is
      lost — no event is acknowledged and no cursor moves — but nothing
      proceeds either until someone fixes the cause and restarts.

  An isolated `:integrity` commit failure reaches neither event: the
  pipeline's per-event fallback records it in `scriba_dead_letters` with
  `error_kind` `"commit:<SQLSTATE>"` and the projection carries on. Only when
  every attempted write in the batch fails that way does it escalate to
  `:halted`.

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

  ## Lag

  `[:scriba, :projection, :lag]` is the one event that fires on a timer
  rather than on traffic, which is the point: a projection that has stopped
  receiving events emits nothing else, and that is exactly when someone wants
  to know how far behind it is. `lag_ms` is how long ago the event at the
  watermark happened, so an idle but caught-up projection reports the age of
  the last event it saw.

  It stays silent until the projection has committed something. A projection
  with no watermark reporting `lag_ms: 0` would read as caught-up when it has
  not started.

  `watermark` in the same measurement is the contiguous global position
  (`Scriba.Watermark`), which is what makes the pair useful for rebuild
  progress as well as alerting.

  Throughput has no event of its own: Broadway already emits one per batch,
  and counting `[:scriba, :projection, :event, :stop]` gives the per-event
  rate without Scriba adding a second number that can disagree.
  """
end
