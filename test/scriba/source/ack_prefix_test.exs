defmodule Scriba.Source.Commanded.AckPrefixTest do
  @moduledoc """
  Which events may be acknowledged, given which ones have committed.

  Acknowledgements against an event store are prefix acknowledgements: acking
  event 7 acks everything up to 7, whether or not it committed. Batches do not
  commit in source order, so the producer may hold a committed 6 and 7 while a
  handler is still working on 5. Acking 7 there moves the store's checkpoint
  past 5, and a crash in that window loses event 5 with no dead letter, no
  cursor anomaly and no log line.

  `drain_contiguous/3` is the arithmetic that prevents it: the longest gapless
  run starting at the oldest unacknowledged event, and nothing past a gap. The
  end-to-end proof against a real EventStore lives in
  `bench/test/ack_loss_test.exs`, which needs a running store; these are the
  same rules at the unit level, where they run on every `mix test`.
  """

  use ExUnit.Case, async: true

  alias Scriba.Source.Commanded

  describe "the gapless prefix" do
    test "nothing committed acknowledges nothing" do
      assert {nil, in_flight, committed} = drain(1..5, [])

      assert positions(in_flight) == [1, 2, 3, 4, 5]
      assert MapSet.size(committed) == 0
    end

    test "a committed prefix acknowledges its last event" do
      assert {event, in_flight, committed} = drain(1..5, [1, 2, 3])

      assert event.event_number == 3
      assert positions(in_flight) == [4, 5]
      assert MapSet.size(committed) == 0
    end

    test "everything committed acknowledges the last event and empties the queue" do
      assert {event, in_flight, committed} = drain(1..5, [1, 2, 3, 4, 5])

      assert event.event_number == 5
      assert :queue.is_empty(in_flight)
      assert MapSet.size(committed) == 0
    end

    test "a gap holds the acknowledgement at the event before it" do
      # 5 is still in a handler; 6 and 7 committed from other streams.
      assert {event, in_flight, committed} = drain(4..7, [4, 6, 7])

      assert event.event_number == 4
      assert positions(in_flight) == [5, 6, 7]

      # 6 and 7 stay recorded — they are committed, just not acknowledgeable.
      assert MapSet.equal?(committed, MapSet.new([6, 7]))
    end

    test "the regression: the highest committed event is not the acknowledged one" do
      {event, _in_flight, _committed} = drain(1..7, [1, 2, 3, 4, 6, 7])

      # Acking 7 here is the bug. It tells the store everything through 7 was
      # handled, including 5, which no one has handled.
      assert event.event_number == 4
      refute event.event_number == 7
    end

    test "committing the blocker releases everything behind it at once" do
      {nil, in_flight, committed} = drain(5..7, [6, 7])

      # 5 lands. One ack for 7 now covers 5, 6 and 7 — which is safe, because
      # all three are accounted for.
      assert {event, drained, remaining} =
               Commanded.drain_contiguous(in_flight, MapSet.put(committed, 5), nil)

      assert event.event_number == 7
      assert :queue.is_empty(drained)
      assert MapSet.size(remaining) == 0
    end

    test "a gap immediately at the head acknowledges nothing" do
      assert {nil, in_flight, committed} = drain(5..7, [6, 7])

      assert positions(in_flight) == [5, 6, 7]
      assert MapSet.equal?(committed, MapSet.new([6, 7]))
    end

    test "two gaps stop at the first one" do
      assert {event, in_flight, _committed} = drain(1..8, [1, 2, 4, 5, 7, 8])

      assert event.event_number == 2
      assert positions(in_flight) == [3, 4, 5, 6, 7, 8]
    end
  end

  describe "degenerate inputs" do
    test "an empty queue acknowledges nothing, whatever is marked committed" do
      assert {nil, in_flight, committed} = drain([], [1, 2, 3])

      assert :queue.is_empty(in_flight)
      assert MapSet.equal?(committed, MapSet.new([1, 2, 3]))
    end

    test "committed numbers not in flight do not advance the prefix" do
      # A redelivery can leave the producer holding acks for events it is no
      # longer tracking. They must not be mistaken for progress.
      assert {nil, in_flight, _committed} = drain(10..12, [1, 2, 3])

      assert positions(in_flight) == [10, 11, 12]
    end

    test "event numbers need not start at one" do
      assert {event, in_flight, _committed} = drain(100..104, [100, 101])

      assert event.event_number == 101
      assert positions(in_flight) == [102, 103, 104]
    end
  end

  ## Helpers

  # The producer records in-flight events as {event_number, event} in delivery
  # order; only the event number is read here, so the event carries nothing
  # else.
  defp drain(numbers, committed) do
    in_flight =
      Enum.reduce(numbers, :queue.new(), fn n, q ->
        :queue.in({n, %{event_number: n}}, q)
      end)

    Commanded.drain_contiguous(in_flight, MapSet.new(committed), nil)
  end

  defp positions(queue) do
    queue |> :queue.to_list() |> Enum.map(fn {position, _event} -> position end)
  end
end
