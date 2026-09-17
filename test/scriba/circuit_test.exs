defmodule Scriba.CircuitTest do
  @moduledoc """
  The failure memory that decides backoff length and whether to halt.

  `Scriba.Circuit` is the only place that answers two questions the producer
  cannot, because it dies between batches: how long to wait before dying
  again, and whether a run of single-event integrity failures is one bad row
  or a schema the handler no longer matches. Both answers are counters, and a
  counter that silently stops counting — or one that leaks between
  projections — turns a halt into a drained stream.

  State is one shared ETS table for the whole BEAM, so every test here uses a
  unique projection key and asserts isolation explicitly rather than relying
  on it.
  """

  use ExUnit.Case, async: true

  alias Scriba.Circuit

  # Mirrors @backoff/@max_backoff in Scriba.Circuit. Duplicated on purpose:
  # reading the module attribute back would assert the code against itself,
  # and this curve is a documented operational contract — 30 restarts spread
  # over roughly twelve minutes is what makes the supervisor's 30-per-60s
  # budget mean what it claims.
  @backoff [0, 100, 500, 1_000, 5_000, 15_000]
  @max_backoff 30_000

  setup do
    {:ok, projection: unique_name()}
  end

  describe "record_transient/2" do
    test "escalates along the documented curve", %{projection: name} do
      delays = Enum.map(@backoff, fn _ -> Circuit.record_transient(name, 1) end)

      assert delays == @backoff
    end

    test "caps at the maximum instead of growing without bound", %{projection: name} do
      for _ <- @backoff, do: Circuit.record_transient(name, 1)

      # Well past the end of the curve: the cap is what bounds a projection
      # that is failing because something outside it is down.
      assert Enum.map(1..20, fn _ -> Circuit.record_transient(name, 1) end) ==
               List.duplicate(@max_backoff, 20)
    end

    test "a commit resets the escalation", %{projection: name} do
      for _ <- 1..4, do: Circuit.record_transient(name, 1)
      assert :ok = Circuit.reset(name, 1)

      assert Circuit.record_transient(name, 1) == hd(@backoff)
    end
  end

  describe "record_wipeout/3" do
    test "more than one event attempted and all failed is systemic at once", %{projection: name} do
      assert Circuit.record_wipeout(name, 1, 2) == :halt
    end

    test "one event attempted is ambiguous, so it dead-letters", %{projection: name} do
      assert Circuit.record_wipeout(name, 1, 1) == :continue
    end

    test "but three single-event batches running with nothing committing is not one bad row",
         %{projection: name} do
      assert Circuit.record_wipeout(name, 1, 1) == :continue
      assert Circuit.record_wipeout(name, 1, 1) == :continue
      assert Circuit.record_wipeout(name, 1, 1) == :halt
    end

    test "the streak stays halted once reached", %{projection: name} do
      for _ <- 1..3, do: Circuit.record_wipeout(name, 1, 1)

      assert Circuit.record_wipeout(name, 1, 1) == :halt
    end

    test "a commit between failures means they were not consecutive", %{projection: name} do
      assert Circuit.record_wipeout(name, 1, 1) == :continue
      assert Circuit.record_wipeout(name, 1, 1) == :continue
      :ok = Circuit.reset(name, 1)

      # The point of the streak is "nothing ever committed". Something did.
      assert Circuit.record_wipeout(name, 1, 1) == :continue
    end
  end

  describe "isolation" do
    test "the two counters do not feed each other", %{projection: name} do
      for _ <- 1..5, do: Circuit.record_transient(name, 1)

      # Transient failures are not evidence about schema mismatch, and a
      # shared counter would halt this projection on its first bad row.
      assert Circuit.record_wipeout(name, 1, 1) == :continue
    end

    test "versions of the same projection count separately", %{projection: name} do
      for _ <- 1..3, do: Circuit.record_transient(name, 1)

      # A rebuild at v2 runs beside v1 and must not inherit its backoff.
      assert Circuit.record_transient(name, 2) == hd(@backoff)
      assert Circuit.record_wipeout(name, 2, 1) == :continue
    end

    test "one projection's streak does not halt another", %{projection: name} do
      other = unique_name()

      assert Circuit.record_wipeout(name, 1, 1) == :continue
      assert Circuit.record_wipeout(name, 1, 1) == :continue
      assert Circuit.record_wipeout(other, 1, 1) == :continue
      assert Circuit.record_wipeout(name, 1, 1) == :halt
    end

    test "resetting one projection leaves the others counting", %{projection: name} do
      other = unique_name()

      for _ <- 1..2, do: Circuit.record_wipeout(other, 1, 1)
      :ok = Circuit.reset(name, 1)

      assert Circuit.record_wipeout(other, 1, 1) == :halt
    end
  end

  test "resetting a projection that never failed is not an error" do
    assert :ok = Circuit.reset(unique_name(), 1)
  end

  defp unique_name, do: "circuit-#{System.unique_integer([:positive])}"
end
