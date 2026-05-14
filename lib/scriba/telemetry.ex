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
  | `[:scriba, :projection, :batch, :stop]` | `Scriba.Projection.Pipeline.handle_batch/4` (manual emit, success branch only) | `duration`, `batch_size` | `projection` |
  | `[:scriba, :projection, :cache_initialized]` | `Scriba.Position.init_cache/3` | `wiped_count`, `preloaded_count` | `name`, `version`, `source` |

  `projection` is `%{name: String.t(), version: pos_integer()}`. `event_type`
  is `event.type` (source-adapter-supplied). `duration` is monotonic-time
  units — pass through `System.convert_time_unit/3` to get ms/µs.

  Batch `:stop` is emitted only when the target's `apply_batch` returns
  `:ok` — i.e. when the Multi committed. Batch failure observability would
  be a separate `:batch, :exception` event, not in scope for v0.1.

  Skipped events (source-side dedup) do NOT emit per-event telemetry —
  there was no handler call to measure.

  ## Out of scope for v0.1

  Lag and throughput events (`[:scriba, :projection, :lag]`,
  `[:scriba, :projection, :throughput]`) are v0.2 per
  `SCRIBA_ARCHITECTURE.md` §2 and `` §3.1. See
  §7.5 of the architecture doc for the implementation note on periodic
  timers that the v0.2 work will need.
  """
end
