defmodule ScribaBench.Wait do
  @moduledoc """
  Polling for conditions that have no telemetry to wait on.

  Every test here drives a real event store and a real database, so the
  interesting states — rows committed, a subscription acquired, a watermark
  caught up — arrive on their own schedule. Each test file had grown its own
  copy of this, which is how four of them ended up with the same recursion
  written four slightly different ways.
  """

  @doc """
  Polls `fun` until it returns true, or the deadline passes.

  Returns whether the condition held, so the caller can `assert` on it and
  say in the failure message what it was waiting for — a bare timeout tells
  you nothing at three in the morning.
  """
  @spec until((-> boolean()), pos_integer(), pos_integer()) :: boolean()
  def until(fun, timeout_ms \\ 30_000, interval_ms \\ 200) do
    do_until(fun, System.monotonic_time(:millisecond) + timeout_ms, interval_ms)
  end

  defp do_until(fun, deadline, interval_ms) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) > deadline ->
        false

      true ->
        Process.sleep(interval_ms)
        do_until(fun, deadline, interval_ms)
    end
  end
end
