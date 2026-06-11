defmodule Scriba.Target do
  @moduledoc """
  Behaviour for read-model targets.

  A target consumes a batch of events together with their handler results and
  applies them atomically alongside per-stream position updates. The Ecto
  target wraps everything in an `Ecto.Multi` and calls `Repo.transaction/1`.
  The Test target updates an in-memory `Agent`. The engine treats both
  uniformly via the `c:apply_batch/5` callback.

  `c:init/1` is called once when the projection starts and returns the state
  threaded through every subsequent `c:apply_batch/5` call. `apply_batch/5`
  returns either `{:ok, state}` on success or `{:error, reason, state}` on
  failure — the engine routes failures to the dead-letter table per §9.

  ## stream_advances invariant

  The `stream_advances` argument is a `%{stream_id => position}` map of the
  highest position the Pipeline has accepted for each stream represented in
  this batch. **Each value is monotonic-non-decreasing per stream across
  successive batches** — the Pipeline does not advance a stream backward.
  Per §9.2, dead-lettered events ALSO advance the cursor — `stream_advances`
  reflects the max across both success and dead-letter results.

  The invariant is preserved by construction (per-stream affinity
  in Broadway's processor partitioning means events for one stream arrive
  at one batcher in source order). Source-side dedup
  also enforces it across crashes by skipping events whose position is at
  or below the stream's committed cursor.

  ## Dead-letter routing

  The Pipeline partitions a batch's handler results into "success-shape"
  (`:skip`, `{:insert, _}`, `{:update, _, _, _}`, `{:delete, _, _}`,
  `{:multi, _}`) and "failure-shape" (`{:error, reason}` and the engine-
  internal `{:exception, exception, stacktrace}` produced when a handler
  raises). Successful events flow to `apply_batch/6` as `events` +
  `handler_results`; failed events flow as `dead_letters`, a list of
  `{event, error}` tuples.

  The Target adapter writes both atomically: read-model writes, dead-
  letter rows, and per-stream cursor advances all commit in one
  transaction (Ecto target) or one in-memory update (Test target). The
  `:error` and `:exception` tags **never** appear in `handler_results` —
  Pipeline strips them at the boundary.
  """

  @type projection :: %{name: String.t(), version: pos_integer()}
  @type state :: term()
  @type handler_result :: term()
  @type stream_advances :: %{String.t() => non_neg_integer()}
  @type dead_letter :: {Scriba.Event.t(), error :: term()}

  @callback init(opts :: keyword()) :: {:ok, state}

  @callback apply_batch(
              events :: [Scriba.Event.t()],
              handler_results :: [handler_result],
              projection :: projection,
              stream_advances :: stream_advances,
              dead_letters :: [dead_letter],
              state :: state
            ) :: {:ok, state} | {:error, reason :: term(), state}
end
