defmodule Scriba.Test.Events do
  @moduledoc false

  alias Scriba.Event

  @doc """
  Generates `n` test events with sequential global positions starting at 1.

  `:streams` (default 1) controls how many distinct stream_ids are used,
  cycling through `stream-0`, `stream-1`, ... so events for each stream
  are interleaved in global order.
  """
  def list(n, opts \\ []) when is_integer(n) and n > 0 do
    streams = Keyword.get(opts, :streams, 1)

    for i <- 1..n do
      %Event{
        id: "evt-#{i}",
        stream_id: "stream-#{rem(i - 1, streams)}",
        type: "test_event",
        data: %{value: i},
        position: i,
        occurred_at: DateTime.utc_now(),
        metadata: %{}
      }
    end
  end
end
