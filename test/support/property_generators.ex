defmodule Scriba.Test.Generators do
  @moduledoc false

  use ExUnitProperties

  alias Scriba.Event

  @doc """
  Lists of `n` events with sequential global positions and randomly-distributed
  stream_ids. Globally monotonic positions imply per-stream monotonic positions
  (per-stream order is a sub-sequence of global order).
  """
  def events_gen(opts \\ []) do
    min_n = Keyword.get(opts, :min, 1)
    max_n = Keyword.get(opts, :max, 50)
    max_streams = Keyword.get(opts, :max_streams, 5)

    gen all n <- StreamData.integer(min_n..max_n),
            stream_count <- StreamData.integer(1..max_streams),
            picks <- StreamData.list_of(StreamData.integer(0..(stream_count - 1)), length: n) do
      build_events(picks)
    end
  end

  defp build_events(picks) do
    picks
    |> Enum.with_index(1)
    |> Enum.map(fn {stream_idx, i} ->
      %Event{
        id: "evt-#{i}",
        stream_id: "stream-#{stream_idx}",
        type: "TestEvent",
        data: %{value: i},
        position: i,
        occurred_at: DateTime.utc_now(),
        metadata: %{}
      }
    end)
  end

  @doc """
  A list of crash delays in milliseconds. `[]` means no crashes.
  """
  def crash_delays_gen(opts \\ []) do
    max_count = Keyword.get(opts, :max, 3)
    min_delay = Keyword.get(opts, :min_delay, 5)
    max_delay = Keyword.get(opts, :max_delay, 100)

    gen all count <- StreamData.integer(0..max_count),
            delays <-
              StreamData.list_of(StreamData.integer(min_delay..max_delay), length: count) do
      delays
    end
  end
end
