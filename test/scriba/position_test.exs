defmodule Scriba.PositionTest do
  use ExUnit.Case, async: true

  alias Scriba.Position

  setup do
    name = "pos-#{:erlang.unique_integer([:positive])}"
    on_exit(fn -> Position.drop_cache(name, 1) end)
    %{name: name, version: 1}
  end

  describe "init_cache/3" do
    test "creates the ETS table empty when no repo is configured", %{name: name, version: v} do
      Position.init_cache(name, v)

      assert Position.stream_positions(name, v) == %{}
      assert Position.safe_position(name, v) == 0
    end

    test "is idempotent — second call does not wipe existing per-stream cursors", %{
      name: name,
      version: v
    } do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 42)
      Position.init_cache(name, v)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 42}
    end

    test "returns the table name", %{name: name, version: v} do
      assert Position.init_cache(name, v) == Position.table_name(name, v)
    end
  end

  describe "table_name/2" do
    test "is deterministic for the same (name, version)", %{name: name, version: v} do
      assert Position.table_name(name, v) == Position.table_name(name, v)
    end

    test "differs across versions", %{name: name} do
      refute Position.table_name(name, 1) == Position.table_name(name, 2)
    end
  end

  describe "cache_get/3" do
    test "returns :error when the cache table does not exist", %{name: name, version: v} do
      assert Position.cache_get(name, v, "any-stream") == :error
    end

    test "returns :error for an unknown stream when the table exists", %{name: name, version: v} do
      Position.init_cache(name, v)

      assert Position.cache_get(name, v, "missing") == :error
    end

    test "returns {:ok, position} after a put", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 17)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 17}
    end
  end

  describe "cache_put/4" do
    test "no-ops silently when the cache table doesn't exist", %{name: name, version: v} do
      assert Position.cache_put(name, v, "s", 5) == :ok
      assert Position.cache_get(name, v, "s") == :error
    end

    test "overwrites the prior value", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 1)
      Position.cache_put(name, v, "stream-a", 100)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 100}
    end

    test "tracks per-stream cursors independently", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 5}
      assert Position.cache_get(name, v, "stream-b") == {:ok, 99}
    end
  end

  describe "stream_positions/2" do
    test "returns an empty map when no streams are tracked", %{name: name, version: v} do
      Position.init_cache(name, v)

      assert Position.stream_positions(name, v) == %{}
    end

    test "returns the full per-stream map", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)

      assert Position.stream_positions(name, v) == %{"stream-a" => 5, "stream-b" => 99}
    end

    test "returns empty map when the table doesn't exist", %{name: name, version: v} do
      assert Position.stream_positions(name, v) == %{}
    end
  end

  describe "safe_position/2" do
    test "returns 0 when the cache is empty", %{name: name, version: v} do
      Position.init_cache(name, v)

      assert Position.safe_position(name, v) == 0
    end

    test "returns 0 when the table doesn't exist", %{name: name, version: v} do
      assert Position.safe_position(name, v) == 0
    end

    test "returns the minimum position across all streams", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)
      Position.cache_put(name, v, "stream-c", 50)

      assert Position.safe_position(name, v) == 5
    end
  end

  describe "drop_cache/2" do
    test "removes the table; subsequent reads miss", %{name: name, version: v} do
      Position.init_cache(name, v)
      Position.cache_put(name, v, "stream-a", 9)
      Position.drop_cache(name, v)

      assert Position.cache_get(name, v, "stream-a") == :error
    end

    test "is idempotent on already-missing tables", %{name: name, version: v} do
      assert Position.drop_cache(name, v) == :ok
      assert Position.drop_cache(name, v) == :ok
    end
  end
end
