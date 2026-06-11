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
  Property_db generator.

  Produces an event list where:

    * `stream_count` ∈ 1..5
    * `n` ∈ max(min, stream_count)..max  (default min: 10, max: 200)
    * Every stream has at least one event — the first `stream_count` picks
      are pinned one-per-stream as anchors (0, 1, …, stream_count-1) so
      coverage is guaranteed.
    * Per-stream order is monotonic by construction: positions are global
      1..n, and any per-stream subsequence inherits that ordering.

  ## :shuffle option

  Default `false`. When false, the anchor picks stay at the front of the
  list — stream-0 always receives event 1, stream-1 event 2, etc. This is
  fine for **PD2 (position consistency)** and **PD3 (cursor resume)**: both
  properties are stated per-stream and don't depend on the global stream
  layout being varied. Stable layout also shrinks faster on failure.

  Pass `shuffle: true` for **PD1 (exactly-once under crash)**. PD1 injects
  crashes at random points in the event stream; if every iteration has
  the same fixed stream layout, the same crash-position always lands at
  the same stream boundary, masking bugs that depend on crash-vs-boundary
  timing variation. The shuffle uses StreamData-generated sort keys so
  the permutation is deterministic for a given test seed.

  `gen all` chains in count-then-content order.
  """
  def events_gen_pd(opts \\ []) do
    min_n = Keyword.get(opts, :min, 10)
    max_n = Keyword.get(opts, :max, 200)
    max_streams = Keyword.get(opts, :max_streams, 5)
    shuffle? = Keyword.get(opts, :shuffle, false)

    gen all stream_count <- StreamData.integer(1..max_streams),
            n <- StreamData.integer(max(min_n, stream_count)..max_n),
            extra_picks <-
              StreamData.list_of(
                StreamData.integer(0..(stream_count - 1)),
                length: n - stream_count
              ),
            sort_keys <- StreamData.list_of(StreamData.integer(0..1_000_000), length: n) do
      anchor_picks = Enum.to_list(0..(stream_count - 1))
      picks = anchor_picks ++ extra_picks

      final_picks =
        if shuffle? do
          picks
          |> Enum.zip(sort_keys)
          |> Enum.sort_by(&elem(&1, 1))
          |> Enum.map(&elem(&1, 0))
        else
          picks
        end

      build_events(final_picks)
    end
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
