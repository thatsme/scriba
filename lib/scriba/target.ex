defmodule Scriba.Target do
  @moduledoc """
  Behaviour for read-model targets.

  A target consumes a batch of events together with their handler results and
  applies them atomically alongside per-stream position updates. The Ecto
  target wraps everything in an `Ecto.Multi` and calls `Repo.transaction/1`.
  The Test target updates an in-memory `Agent`. The engine treats both
  uniformly via the `c:apply_batch/6` callback.

  ## The target is the transaction boundary

  A target owns three writes — the read-model rows, the per-stream cursor
  advances, and any dead letters — and commits them together or not at all.
  That is not an accident of the Ecto implementation; it is the guarantee.
  Splitting cursor and dead-letter storage into a separate behaviour would
  either leave them in the same transaction anyway, or turn effectively-once
  delivery into at-least-once with a crash window.

  So the boundary stays where it is, and the consequence is stated plainly: a
  store that cannot commit those three things in one transaction cannot
  provide Scriba's delivery guarantee.

  ## Other targets

  Only `Scriba.Target.Ecto` ships. Another one is perfectly implementable —
  `Scriba.Position.multi/5` and `Scriba.DeadLetter.multi/4` are public so an
  Ecto-based target reuses the cursor and dead-letter SQL rather than
  reimplementing it — and if you need one, **open an issue**. It will be
  built with you rather than ahead of you.

  That is deliberate, not evasive. An interface with one implementation
  behind it encodes that implementation's assumptions: `Scriba.Source`
  looked correct until a real event store showed that acknowledgement had
  been broken since 0.1.0, because the in-memory adapter the tests used
  could not express the bug. A second target is what would reveal the right
  shape here, and guessing at it first buys complexity, not readiness.

  `c:init/1` is called once when the projection starts and returns the state
  threaded through every subsequent `c:apply_batch/6` call. `apply_batch/6`
  returns either `{:ok, state}` on success or `{:error, reason, state}` on
  failure. What happens next depends on how `Scriba.Failure` classifies the
  error: a transient one marks the whole batch failed via
  `Broadway.Message.failed/2` and replays, a structural one halts the
  projection, and an integrity one is isolated by retrying the batch event by
  event — those events *are* dead-lettered, in a transaction of their own,
  with `error_kind` `"commit:<SQLSTATE>"`.

  On the replay path recovery is the source's responsibility. Nothing in
  the batch is acknowledged — not even the messages that succeeded, since
  event-store acks are prefix-acks and cannot express a gap. The source is
  then expected to force redelivery from its last durable checkpoint;
  `Scriba.Source.Commanded` does this by stopping its producer, which rewinds
  the subscription (see `Scriba.BatchCommitError`). Events that did commit are
  filtered by source-side dedup on the way back through.

  Dead-lettering covers per-event handler failures (see below) and
  integrity-class commit failures. It never covers transient or structural
  ones: no row can be written for a database that is unreachable, and a
  schema that does not match the code would drain every event into
  `scriba_dead_letters` over an empty read model.

  > #### Verification status {: .warning}
  >
  > The replay half of this — source refuses to ack, forces redelivery, dedup
  > filters what committed — is exercised through `Scriba.Test.Source`, which
  > requeues failed messages in position order, and against real Postgres for
  > the integrity and structural classes. It has **not** been run against a
  > real event store, so conservation across an event-store outage is
  > unverified. See `Scriba.BatchCommitError`.

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

  @doc """
  Returns whether this target can apply `result`.

  Optional. When a target does not export it, every non-failure handler return
  is accepted — which is what `Scriba.Target.Test` wants, since it records any
  non-`:skip` result without interpreting it.

  Implement it when the target has a fixed result vocabulary, as
  `Scriba.Target.Ecto` does with §4.2's six shapes. The Pipeline consults this
  *before* assembling a batch and routes anything rejected to the dead-letter
  table as a per-event failure, with `error_kind` `"invalid_return"`.

  This exists so a malformed handler return cannot reach `c:apply_batch/6`.
  If it does, Multi assembly raises outside the target's own
  `case repo.transaction(...)`, Broadway fails the whole batch, and the source
  refuses to acknowledge it — correct behaviour for a commit failure, but a
  permanent crash loop when the cause is one deterministic bad return that
  will fail identically on every redelivery.

  Keep it total and side-effect free: it runs once per event.
  """
  @callback valid_result?(result :: handler_result) :: boolean()

  @optional_callbacks valid_result?: 1

  @callback apply_batch(
              events :: [Scriba.Event.t()],
              handler_results :: [handler_result],
              projection :: projection,
              stream_advances :: stream_advances,
              dead_letters :: [dead_letter],
              state :: state
            ) :: {:ok, state} | {:error, reason :: term(), state}
end
