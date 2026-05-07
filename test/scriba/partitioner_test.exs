defmodule Scriba.PartitionerTest do
  use ExUnit.Case, async: true

  alias Scriba.Partitioner

  describe "partition/2" do
    test "is deterministic — same input always maps to same partition" do
      stream_id = "user-123"
      partitions = 16
      result = Partitioner.partition(stream_id, partitions)

      for _ <- 1..100 do
        assert Partitioner.partition(stream_id, partitions) == result
      end
    end

    test "returns a value in 0..partitions-1" do
      partitions = 16

      for stream_id <- [
            "a",
            "ab",
            "user-123",
            "order-#{:erlang.unique_integer()}",
            String.duplicate("x", 256),
            ""
          ] do
        result = Partitioner.partition(stream_id, partitions)
        assert is_integer(result)
        assert result >= 0
        assert result < partitions
      end
    end

    test "works for parallelism=1 (always partition 0)" do
      for stream_id <- ["a", "b", "c", "anything"] do
        assert Partitioner.partition(stream_id, 1) == 0
      end
    end

    test "raises on non-positive partition counts" do
      assert_raise FunctionClauseError, fn -> Partitioner.partition("a", 0) end
      assert_raise FunctionClauseError, fn -> Partitioner.partition("a", -1) end
      assert_raise FunctionClauseError, fn -> Partitioner.partition("a", 1.5) end
    end

    test "distribution is roughly even across partitions" do
      partitions = 16
      sample_size = 10_000
      stream_ids = for i <- 1..sample_size, do: "stream-#{i}"

      counts =
        Enum.reduce(stream_ids, %{}, fn s, acc ->
          Map.update(acc, Partitioner.partition(s, partitions), 1, &(&1 + 1))
        end)

      assert map_size(counts) == partitions, "every partition should receive at least one event"

      expected = sample_size / partitions
      tolerance = expected * 0.25

      for {partition, count} <- counts do
        assert_in_delta count,
                        expected,
                        tolerance,
                        "partition #{partition} drifted: got #{count}, expected ~#{expected}"
      end
    end

    test "different partition counts give different mappings for the same stream_id" do
      stream_id = "user-123"

      mappings =
        for n <- [4, 8, 16, 32, 64], into: %{} do
          {n, Partitioner.partition(stream_id, n)}
        end

      assert map_size(mappings) == 5
    end
  end
end
