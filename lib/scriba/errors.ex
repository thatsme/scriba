defmodule Scriba.Errors do
  @moduledoc false
end

defmodule Scriba.BatchCommitError do
  @moduledoc """
  Raised in a source's producer process when a batch failed to commit.

  This is deliberately a crash, not a handled error. A commit failure cannot
  be dead-lettered — writing the dead-letter row needs the same transaction
  that just failed — and it cannot be selectively skipped, because event-store
  acks are prefix-acks: acknowledging event N acknowledges everything before
  it, so a gap cannot be preserved.

  The only correct response is to stop acknowledging and let the producer die.
  The event store rewinds the subscription to the last durable checkpoint when
  the subscriber process goes down, the supervisor restarts the producer, and
  the batch is redelivered. Events that did commit are filtered by source-side
  dedup on the way back through.

  ## This is the transient path only

  Restarting to replay is correct when the failure is transient — the database
  is unavailable, a deadlock was detected, a query was cancelled. It is *not*
  correct when the failure is deterministic, because replaying reproduces it
  identically and the loop never ends.

  `Scriba.Failure` makes that distinction from SQLSTATE before this error is
  ever raised, so the two deterministic classes never reach here:

    * `:integrity` — isolated by the Pipeline's per-event fallback and
      dead-lettered, so the projection keeps moving.
    * `:structural` — halts the projection loudly via
      `[:scriba, :projection, :halted]`; the source is told not to restart.

  A restart loop against a genuinely unavailable database is therefore the
  correct observable behaviour for what remains. A silent stall is not, and
  neither is an infinite loop against a bad column name.

  ## Verification status (v0.1)

  This mechanism — refuse to ack, kill the producer, rewind the subscription,
  back off, replay — is covered by **unit tests only**. It has never been
  exercised against a real event store.

  Scriba's own test double, `Scriba.Test.Source`, discards failed messages
  instead of redelivering them, so the suite cannot replay anything and cannot
  close the conservation identity across an outage. Measuring it requires a
  persistent event store. Treat the paragraphs above as the design, not as
  observed behaviour.
  """

  defexception [:count, :reason]

  @impl Exception
  def message(%__MODULE__{count: count, reason: reason}) do
    """
    #{count} message(s) in a batch failed to commit; refusing to acknowledge.

    Reason: #{inspect(reason)}

    The producer is stopping so the subscription rewinds to its last durable
    checkpoint and the batch is redelivered. If this repeats, the underlying
    target (usually Postgres) is failing — fix it there rather than at the
    projection level.
    """
  end
end
