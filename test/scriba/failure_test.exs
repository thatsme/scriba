defmodule Scriba.FailureTest do
  @moduledoc """
  The classifier replaced a "did some events succeed?" heuristic that was
  wrong in both directions. These tests pin the two counterexamples that
  killed it, because they are the cases a future refactor is most likely to
  reintroduce.
  """

  use ExUnit.Case, async: true

  alias Scriba.Failure

  defp pg(code, name) do
    %Postgrex.Error{postgres: %{pg_code: code, code: name, severity: "ERROR", message: "boom"}}
  end

  describe "classify/1 — integrity (dead-letter the event, advance)" do
    test "class 23 constraint violations" do
      assert Failure.classify(pg("23505", :unique_violation)) == :integrity
      assert Failure.classify(pg("23502", :not_null_violation)) == :integrity
      assert Failure.classify(pg("23503", :foreign_key_violation)) == :integrity
      assert Failure.classify(pg("23514", :check_violation)) == :integrity
    end

    test "class 22 data exceptions — bad data in this event, not a bad schema" do
      assert Failure.classify(pg("22003", :numeric_value_out_of_range)) == :integrity
      assert Failure.classify(pg("22001", :string_data_right_truncation)) == :integrity
    end

    test "Ecto.ConstraintError — the usual form, since {:insert, struct} declares no constraints" do
      err = %Ecto.ConstraintError{type: :unique, constraint: "x_pkey", message: "boom"}
      assert Failure.classify(err) == :integrity
    end
  end

  describe "classify/1 — transient (replay)" do
    test "the codes that partial-success would have mislabelled as poison" do
      # This is counterexample #1: resource pressure fails non-uniformly, so a
      # per-event pass sees some events succeed as pressure eases. Reading
      # that as "deterministic" would dead-letter good events and advance past
      # them — data loss from a blip.
      assert Failure.classify(pg("40001", :serialization_failure)) == :transient
      assert Failure.classify(pg("40P01", :deadlock_detected)) == :transient
      assert Failure.classify(pg("53300", :too_many_connections)) == :transient
      assert Failure.classify(pg("53200", :out_of_memory)) == :transient
      assert Failure.classify(pg("57014", :query_canceled)) == :transient
      assert Failure.classify(pg("08006", :connection_failure)) == :transient
    end

    test "class 57 operator intervention — a restarting database, not a broken one" do
      # Regression for the single worst misclassification found in this work.
      # `docker stop` emits 57P01, which was reaching :structural and halting
      # the projection permanently — i.e. every routine Postgres restart or
      # failover needed a human. Found by E3 in one millisecond; invisible to
      # every amount of reading beforehand.
      assert Failure.classify(pg("57P01", :admin_shutdown)) == :transient
      assert Failure.classify(pg("57P02", :crash_shutdown)) == :transient
      assert Failure.classify(pg("57P03", :cannot_connect_now)) == :transient
      assert Failure.classify(pg("57014", :query_canceled)) == :transient
    end

    test "a failover that lands on a read-only replica" do
      assert Failure.classify(pg("25006", :read_only_sql_transaction)) == :transient
    end

    test "DBConnection errors" do
      assert Failure.classify(%DBConnection.ConnectionError{message: "gone"}) == :transient
    end

    test "a Postgrex.Error carrying no :postgres map" do
      # Protocol-level and some connection-level failures arrive with
      # postgres: nil. These previously matched no clause and fell through to
      # `classify(_other)`, halting the projection permanently on what was a
      # reconnectable blip. Recognisably a transport problem, so it is
      # transient by decision rather than by fallthrough.
      assert Failure.classify(%Postgrex.Error{postgres: nil, message: "closed"}) == :transient
    end
  end

  describe "classify/1 — structural (halt loudly)" do
    test "the codes that partial-success would have mislabelled as transient" do
      # Counterexample #2: handler code deployed ahead of its migration fails
      # on EVERY event, deterministically. Reading uniform failure as
      # "environmental" replays it forever — the exact loop this work exists
      # to remove.
      assert Failure.classify(pg("42703", :undefined_column)) == :structural
      assert Failure.classify(pg("42P01", :undefined_table)) == :structural
      assert Failure.classify(pg("42501", :insufficient_privilege)) == :structural
    end

    test "a dropped database is not a restarting one" do
      # The class-57 exception: 57P04 shares its class with admin_shutdown but
      # replaying cannot bring back a database that no longer exists.
      assert Failure.classify(pg("57P04", :database_dropped)) == :structural
    end

    test "unrecognised failures halt rather than guess" do
      assert Failure.classify(:some_random_atom) == :structural
      assert Failure.classify(%RuntimeError{message: "?"}) == :structural
      assert Failure.classify(pg("XX000", :internal_error)) == :structural
    end
  end

  describe "class 40 is not transient wholesale" do
    test "only the retryable members are listed" do
      # 40002 (transaction_integrity_constraint_violation) shares the class
      # with serialization_failure but is not retryable. Classifying the whole
      # class as transient would loop on it.
      assert Failure.classify(pg("40002", :transaction_integrity_constraint_violation)) ==
               :structural
    end
  end

  describe "label/1" do
    test "surfaces the SQLSTATE, which is what an operator searches for" do
      assert Failure.label(pg("23505", :unique_violation)) == "23505 (unique_violation)"
    end

    test "names the constraint when Ecto swallowed the SQLSTATE" do
      # E3 showed this is the common integrity case, not an edge one: for
      # `{:insert, struct}` Ecto intercepts the Postgrex error and raises its
      # own, which carries no SQLSTATE. The constraint name is what remains
      # and what an operator can act on.
      assert Failure.label(%Ecto.ConstraintError{
               type: :unique,
               constraint: "test_read_models_pkey",
               message: "m"
             }) == "Ecto.ConstraintError (unique: test_read_models_pkey)"
    end
  end
end
