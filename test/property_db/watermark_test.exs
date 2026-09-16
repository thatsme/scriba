defmodule Scriba.PropertyDb.WatermarkTest do
  @moduledoc """
  `Scriba.Watermark` against real Postgres.

  The behaviour worth pinning is the direction the number can move: forwards
  on its own, never backwards on a late write from a producer that should
  already be gone. `GREATEST` in SQL is what enforces that, so it has to be
  asserted against a database rather than a double.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo
  alias Scriba.Watermark

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  defp projection do
    %{name: "wm-#{System.unique_integer([:positive])}", version: 1}
  end

  test "a projection with no watermark reads as nil" do
    assert Watermark.get(Repo, projection()) == nil
    assert Watermark.lag_ms(Repo, projection()) == nil
  end

  test "records a position and reads it back" do
    p = projection()
    at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    :ok = Watermark.put(Repo, p, 42, at)

    assert %{position: 42, occurred_at: ^at} = Watermark.get(Repo, p)
  end

  test "advances" do
    p = projection()

    :ok = Watermark.put(Repo, p, 10, DateTime.utc_now())
    :ok = Watermark.put(Repo, p, 11, DateTime.utc_now())

    assert %{position: 11} = Watermark.get(Repo, p)
  end

  test "a late write from an older position does not move it backwards" do
    p = projection()
    newer = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    :ok = Watermark.put(Repo, p, 100, newer)
    # A producer that lingered through a handover, reporting where it had got
    # to. Accepting this would send a replica back to 50 and skip 51..100.
    :ok = Watermark.put(Repo, p, 50, DateTime.utc_now())

    assert %{position: 100, occurred_at: occurred} = Watermark.get(Repo, p)
    assert occurred == newer
  end

  test "projections do not see each other's watermarks" do
    a = projection()
    b = projection()

    :ok = Watermark.put(Repo, a, 7, DateTime.utc_now())

    assert %{position: 7} = Watermark.get(Repo, a)
    assert Watermark.get(Repo, b) == nil
  end

  test "versions of the same projection are independent" do
    %{name: name} = p = projection()
    v2 = %{name: name, version: 2}

    :ok = Watermark.put(Repo, p, 500, DateTime.utc_now())
    :ok = Watermark.put(Repo, v2, 3, DateTime.utc_now())

    assert %{position: 500} = Watermark.get(Repo, p)
    assert %{position: 3} = Watermark.get(Repo, v2)
  end

  test "lag is measured from the event's own timestamp" do
    p = projection()
    five_seconds_ago = DateTime.add(DateTime.utc_now(), -5, :second)

    :ok = Watermark.put(Repo, p, 1, five_seconds_ago)

    lag = Watermark.lag_ms(Repo, p)
    assert lag >= 5_000
    assert lag < 10_000
  end

  test "lag is nil when the source reported no timestamp" do
    p = projection()

    :ok = Watermark.put(Repo, p, 1, nil)

    assert %{position: 1, occurred_at: nil} = Watermark.get(Repo, p)
    assert Watermark.lag_ms(Repo, p) == nil
  end
end
