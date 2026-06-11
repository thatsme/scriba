defmodule Scriba.PositionTest do
  use ExUnit.Case, async: true

  alias Scriba.Position

  # The shared cache table is created by Scriba.Supervisor.init/1 when the
  # application starts (which mix test does automatically). Per-test
  # cleanup wipes only this projection's rows.

  setup do
    name = "pos-#{:erlang.unique_integer([:positive])}"
    on_exit(fn -> Position.drop_cache(name, 1) end)
    %{name: name, version: 1}
  end

  describe "shared table" do
    test "create_shared_table/0 is idempotent" do
      assert Position.create_shared_table() == :ok
      assert Position.create_shared_table() == :ok
    end

    test "cache_table/0 returns the named atom of the shared table" do
      assert Position.cache_table() == Scriba.Position.Cache
      assert :ets.whereis(Position.cache_table()) != :undefined
    end
  end

  describe "init_cache/3" do
    test "starts the projection's view of the cache empty (no repo)", %{
      name: name,
      version: v
    } do
      Position.init_cache(name, v)

      assert Position.stream_positions(name, v) == %{}
      assert Position.safe_position(name, v) == 0
    end

    test "wipes prior entries for this projection on each call", %{name: name, version: v} do
      Position.cache_put(name, v, "stream-a", 42)
      Position.cache_put(name, v, "stream-b", 100)
      assert Position.stream_positions(name, v) == %{"stream-a" => 42, "stream-b" => 100}

      Position.init_cache(name, v)

      assert Position.stream_positions(name, v) == %{}
    end

    test "does not wipe other projections' entries", %{name: name, version: v} do
      other = "pos-other-#{:erlang.unique_integer([:positive])}"
      on_exit(fn -> Position.drop_cache(other, 1) end)

      Position.cache_put(other, 1, "stream-a", 999)
      Position.init_cache(name, v)

      assert Position.cache_get(other, 1, "stream-a") == {:ok, 999}
    end

    test "emits cache_initialized telemetry with wiped/preloaded counts", %{
      name: name,
      version: v
    } do
      ref = make_ref()
      handler_id = {:cache_init_test, ref}

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :cache_initialized],
        &__MODULE__.forward_telemetry/4,
        %{test_pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      # Pre-populate so wipe has something to count.
      Position.cache_put(name, v, "stream-a", 1)
      Position.cache_put(name, v, "stream-b", 2)

      Position.init_cache(name, v)

      assert_receive {^ref, %{wiped_count: 2, preloaded_count: 0}, %{source: :empty} = meta},
                     200

      assert meta.name == name
      assert meta.version == v
    end
  end

  # Named telemetry handler — :telemetry warns about anonymous-function
  # handlers (performance note). Using a module function keeps the test
  # output clean and matches what Scriba.Telemetry.Handler will
  # do.
  @doc false
  def forward_telemetry(_event, measurements, metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, measurements, metadata})
  end

  describe "cache_get/3" do
    test "returns :error when no entry exists for the stream", %{name: name, version: v} do
      assert Position.cache_get(name, v, "missing-stream") == :error
    end

    test "returns {:ok, position} after a put", %{name: name, version: v} do
      Position.cache_put(name, v, "stream-a", 17)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 17}
    end

    test "scopes by (name, version, stream_id) — no cross-projection leakage", %{
      name: name,
      version: v
    } do
      other = "pos-other-#{:erlang.unique_integer([:positive])}"
      on_exit(fn -> Position.drop_cache(other, 1) end)

      Position.cache_put(name, v, "stream-a", 1)
      Position.cache_put(other, 1, "stream-a", 999)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 1}
      assert Position.cache_get(other, 1, "stream-a") == {:ok, 999}
    end
  end

  describe "cache_put/4" do
    test "overwrites the prior value", %{name: name, version: v} do
      Position.cache_put(name, v, "stream-a", 1)
      Position.cache_put(name, v, "stream-a", 100)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 100}
    end

    test "tracks per-stream cursors independently", %{name: name, version: v} do
      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 5}
      assert Position.cache_get(name, v, "stream-b") == {:ok, 99}
    end
  end

  describe "stream_positions/2" do
    test "returns an empty map when no streams are tracked for this projection", %{
      name: name,
      version: v
    } do
      assert Position.stream_positions(name, v) == %{}
    end

    test "returns only this projection's entries", %{name: name, version: v} do
      other = "pos-other-#{:erlang.unique_integer([:positive])}"
      on_exit(fn -> Position.drop_cache(other, 1) end)

      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)
      Position.cache_put(other, 1, "stream-c", 50)

      assert Position.stream_positions(name, v) == %{"stream-a" => 5, "stream-b" => 99}
    end
  end

  describe "safe_position/2" do
    test "returns 0 when the projection has no streams", %{name: name, version: v} do
      assert Position.safe_position(name, v) == 0
    end

    test "returns the minimum position across this projection's streams", %{
      name: name,
      version: v
    } do
      Position.cache_put(name, v, "stream-a", 5)
      Position.cache_put(name, v, "stream-b", 99)
      Position.cache_put(name, v, "stream-c", 50)

      assert Position.safe_position(name, v) == 5
    end
  end

  describe "drop_cache/2" do
    test "returns the count of deleted rows", %{name: name, version: v} do
      Position.cache_put(name, v, "stream-a", 9)
      Position.cache_put(name, v, "stream-b", 10)

      assert Position.drop_cache(name, v) == 2
      assert Position.cache_get(name, v, "stream-a") == :error
    end

    test "is 0 when nothing to drop", %{name: name, version: v} do
      assert Position.drop_cache(name, v) == 0
    end

    test "scoped — only this projection's rows deleted", %{name: name, version: v} do
      other = "pos-other-#{:erlang.unique_integer([:positive])}"
      on_exit(fn -> Position.drop_cache(other, 1) end)

      Position.cache_put(name, v, "stream-a", 1)
      Position.cache_put(other, 1, "stream-x", 999)

      Position.drop_cache(name, v)

      assert Position.cache_get(name, v, "stream-a") == :error
      assert Position.cache_get(other, 1, "stream-x") == {:ok, 999}
    end
  end
end
