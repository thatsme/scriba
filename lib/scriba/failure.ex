defmodule Scriba.Failure do
  @moduledoc """
  Classifies a failed commit into one of three kinds, so the engine can pick a
  response that terminates.

  This exists because "did some events succeed?" is not a usable proxy for
  "is this failure deterministic?". Partial success measures *uniformity*, not
  determinism, and it is wrong in both directions:

    * Pool exhaustion, a statement timeout under load, a serialization failure
      or a failover mid-batch all produce *partial* failure as pressure eases.
      Reading that as "deterministic" would dead-letter perfectly good events
      and advance past them — data loss caused by a transient blip.

    * Handler code deployed ahead of its migration produces
      `undefined column` on *every* event in the batch, forever. Reading
      uniform failure as "environmental" replays it in an infinite loop.

  Postgres already publishes the answer as SQLSTATE, so the engine reads that
  rather than guessing. See
  https://www.postgresql.org/docs/current/errcodes-appendix.html.

  ## The three kinds

    * `:integrity` — the event's data is bad against the current schema
      (SQLSTATE class 23, plus class 22 data exceptions such as numeric
      overflow, plus `Ecto.ConstraintError` and invalid changesets).
      Deterministic and specific to one event: it will fail identically on
      every redelivery. **Dead-letter it and advance.**

    * `:transient` — the database is unreachable, busy, restarting, or asked
      us to back off. **Classes 08** (connection exceptions), **53**
      (insufficient resources) and **57** (operator intervention: shutdown,
      restart, failover, still-starting-up), plus `40001` and `40P01`
      (serialization failure, deadlock), `25006` (a write that landed on a
      read-only replica mid-failover), any `Postgrex.Error` carrying no
      SQLSTATE, and `DBConnection` errors. Nothing about the event is wrong.
      **Fail and replay.**

      Class 57 is here because of E3: `docker stop` emits `57P01`
      (admin_shutdown), which an earlier version of this module — listing
      `57014` alone out of that class — sent to `:structural` and halted the
      projection permanently over a routine database restart. `57P04`
      (database_dropped) is carved back out as `:structural`, because a
      dropped database is not a restarted one.

    * `:structural` — the schema or permissions do not match the code
      (class 42), *and everything unrecognised*. Neither response is safe
      here: dead-lettering destroys a projection's worth of events over a
      fixable deploy-ordering mistake, and replaying loops forever.
      **Halt, loudly.** A stall that announces itself is a legitimate
      outcome; the failure mode this library had was that it was silent.

  Unrecognised failures classify as `:structural` deliberately. Halting on
  something we cannot name is recoverable by a human; guessing is not.

  ## Your pool settings decide whether this module runs at all

  `DBConnection` blocks on connection checkout rather than erroring, for up to
  `:queue_target` / `:queue_interval`. A database blip shorter than that window
  is absorbed beneath Scriba entirely: no error is raised, nothing is
  classified, and the projection simply pauses and continues.

  That is good behaviour and Scriba does not try to pre-empt it. But it means
  pool configuration, not this module, determines where the boundary sits
  between "absorbed silently" and "classified and replayed" — and therefore
  whether the escalating backoff in `Scriba.Circuit` ever engages. A repo
  tuned with an aggressive `:queue_target` will reach this code on outages
  that a default-tuned repo never notices.
  """

  @type kind :: :integrity | :transient | :structural

  # SQLSTATE classes (first two characters).
  @integrity_classes ~w(22 23)

  # Class 08 — connection exceptions. Class 53 — insufficient resources.
  # Class 57 — operator intervention: a database being shut down, restarted,
  # failed over or still starting up. **This class is why E3 exists.** It was
  # originally represented here by `57014` alone, on the reasoning that class
  # 57 was too broad to take wholesale. A `docker stop` then produced `57P01`
  # (admin_shutdown), which fell through to `:structural` and halted the
  # projection permanently — a routine database restart requiring human
  # intervention. Static reading could not have found that; the first real
  # fault found it in one millisecond.
  @transient_classes ~w(08 53 57)

  # Individually-listed codes whose class as a whole does not share their
  # meaning: class 40 also covers non-retryable transaction errors, and
  # `25006` is a write against a read-only transaction, which is what a
  # failover to a replica looks like from the writer's side.
  @transient_codes ~w(40001 40P01 25006)

  # Class-57 exception. The database was not restarted, it was dropped —
  # replaying cannot bring it back and dead-lettering would discard events
  # against a target that no longer exists.
  @structural_codes ~w(57P04)

  @doc """
  Classifies a commit failure reason.

  Accepts what `c:Scriba.Target.apply_batch/6` can report: a `Postgrex.Error`,
  an `Ecto.ConstraintError`, an invalid `Ecto.Changeset`, a `DBConnection`
  error, or any other term.
  """
  @spec classify(term()) :: kind()
  def classify(%{__struct__: Postgrex.Error, postgres: %{pg_code: pg_code}})
      when is_binary(pg_code) do
    cond do
      pg_code in @structural_codes -> :structural
      pg_code in @transient_codes -> :transient
      String.slice(pg_code, 0, 2) in @integrity_classes -> :integrity
      String.slice(pg_code, 0, 2) in @transient_classes -> :transient
      true -> :structural
    end
  end

  # A Postgrex.Error carrying no :postgres map — protocol-level failures and
  # some connection-level ones. Recognisably a database transport problem with
  # no SQLSTATE to read, so it is transient by decision, not by fallthrough.
  #
  # Without this clause it reached `classify(_other)` and halted the
  # projection permanently on what was a reconnectable blip. The moduledoc's
  # "unrecognised means halt" reasoning covers failures we cannot identify;
  # this one we can.
  def classify(%{__struct__: Postgrex.Error}), do: :transient

  # Raised by Ecto when a constraint fires and the changeset did not declare
  # it. Scriba's `{:insert, struct}` shape passes a bare struct, so this is
  # the usual form an integrity violation takes — and it carries no SQLSTATE,
  # only the constraint type.
  def classify(%{__struct__: Ecto.ConstraintError}), do: :integrity

  # An operation returned `{:error, changeset}`. The data did not validate;
  # replaying cannot change that.
  def classify(%{__struct__: Ecto.Changeset}), do: :integrity

  def classify(%{__struct__: DBConnection.ConnectionError}), do: :transient

  # Unnamed failure — halt rather than guess. See moduledoc.
  def classify(_other), do: :structural

  @doc """
  A short label for telemetry and dead-letter rows. Returns the SQLSTATE when
  one is available, since that is what an operator searches for.
  """
  @spec label(term()) :: String.t()
  def label(%{__struct__: Postgrex.Error, postgres: %{pg_code: pg_code, code: code}}) do
    "#{pg_code} (#{code})"
  end

  # Ecto intercepts constraint violations and raises its own error, which
  # carries no SQLSTATE — so the commonest integrity case, `{:insert, struct}`
  # hitting a duplicate key, never reaches the clause above. Verified against
  # real Postgres in test/property_db/e3_fault_injection_test.exs, which is
  # also where the assumption that it *would* was corrected.
  #
  # The constraint name is the next best identifier, and it is the one an
  # operator can act on.
  def label(%{__struct__: Ecto.ConstraintError, type: type, constraint: constraint}) do
    "Ecto.ConstraintError (#{type}: #{constraint})"
  end

  def label(%{__struct__: struct_name}), do: inspect(struct_name)
  def label(other), do: inspect(other)
end
