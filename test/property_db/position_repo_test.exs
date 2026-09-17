defmodule Scriba.PropertyDb.PositionRepoTest do
  @moduledoc """
  The half of `Scriba.Position` that talks to Postgres.

  `test/scriba/position_test.exs` covers the ETS side and says so in its own
  header: it never passes a `:repo`. That left the paths that make the cache
  trustworthy after a restart unexercised — the preload the Coordinator runs
  from `init/1`, and the per-stream lazy load the Pipeline falls back to when
  dedup asks about a stream the preload did not reach.

  Both matter for the same reason. The cache is what dedup reads, and a cache
  that comes up empty or stale does not fail loudly: it silently stops
  recognising events that already committed.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Position
  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  setup do
    name = "posrepo-#{System.unique_integer([:positive])}"
    on_exit(fn -> Position.drop_cache(name, 1) end)

    {:ok, name: name, version: 1}
  end

  describe "init_cache/3 with a repo" do
    test "preloads committed cursors from Postgres", %{name: name, version: v} do
      write_position(name, v, "stream-a", 7)
      write_position(name, v, "stream-b", 12)

      # The cache starts cold, exactly as it does in a freshly started
      # Coordinator that has never seen these streams.
      assert Position.cache_get(name, v, "stream-a") == :error

      :ok = Position.init_cache(name, v, repo: Repo)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 7}
      assert Position.cache_get(name, v, "stream-b") == {:ok, 12}
    end

    test "reports what it preloaded", %{name: name, version: v} do
      write_position(name, v, "stream-a", 1)
      write_position(name, v, "stream-b", 2)

      ref = attach_cache_telemetry(name)
      :ok = Position.init_cache(name, v, repo: Repo)

      assert_receive {^ref, measurements, metadata}, 500
      assert measurements.preloaded_count == 2
      assert metadata.source == :postgres
    end

    test "without a repo the cache stays empty even though Postgres has rows", %{
      name: name,
      version: v
    } do
      write_position(name, v, "stream-a", 7)

      ref = attach_cache_telemetry(name)
      :ok = Position.init_cache(name, v)

      assert_receive {^ref, measurements, metadata}, 500
      assert measurements.preloaded_count == 0
      assert metadata.source == :empty
      assert Position.cache_get(name, v, "stream-a") == :error
    end

    test "the preload replaces what was cached, rather than merging with it", %{
      name: name,
      version: v
    } do
      write_position(name, v, "stream-a", 7)

      # A stream the previous incarnation cached and Postgres has no row for
      # must not survive: after a rebuild that truncated the table, a
      # surviving entry would make dedup skip events that need reapplying.
      Position.cache_put(name, v, "stale-stream", 99)

      :ok = Position.init_cache(name, v, repo: Repo)

      assert Position.cache_get(name, v, "stale-stream") == :error
      assert Position.cache_get(name, v, "stream-a") == {:ok, 7}
    end

    test "loads only this projection's version", %{name: name, version: v} do
      write_position(name, v, "stream-a", 7)
      write_position(name, 2, "stream-a", 500)

      :ok = Position.init_cache(name, v, repo: Repo)

      assert Position.cache_get(name, v, "stream-a") == {:ok, 7}
      assert Position.cache_get(name, 2, "stream-a") == :error
    end

    test "a repo that cannot be reached raises rather than coming up empty", %{
      name: name,
      version: v
    } do
      # This runs in Coordinator.init/1, so raising takes the projection down
      # at boot. That is the right direction: a projection whose database is
      # unreachable cannot commit anything either, and an empty cache that
      # looks successful is the failure that hides.
      assert_raise RuntimeError, fn ->
        Position.init_cache(name, v, repo: Scriba.Test.NoSuchRepo)
      end
    end
  end

  describe "cache_get/4 lazy load" do
    test "falls back to Postgres on a cache miss and backfills", %{name: name, version: v} do
      write_position(name, v, "stream-a", 42)

      assert Position.cache_get(name, v, "stream-a") == :error
      assert Position.cache_get(name, v, "stream-a", repo: Repo) == {:ok, 42}

      # Backfilled: the next read needs no query.
      assert Position.cache_get(name, v, "stream-a") == {:ok, 42}
    end

    test "a stream with no committed row reads as a miss", %{name: name, version: v} do
      assert Position.cache_get(name, v, "never-seen", repo: Repo) == :error

      # And nothing is cached for it, so a later commit is free to set it.
      assert Position.cache_get(name, v, "never-seen") == :error
    end

    test "the cached value wins over Postgres", %{name: name, version: v} do
      write_position(name, v, "stream-a", 42)
      Position.cache_put(name, v, "stream-a", 100)

      # The cache is written after a commit and Postgres within it, so a
      # cached value is never behind. Re-reading Postgres here would move the
      # cursor backwards.
      assert Position.cache_get(name, v, "stream-a", repo: Repo) == {:ok, 100}
    end

    test "without a repo a miss stays a miss", %{name: name, version: v} do
      write_position(name, v, "stream-a", 42)

      assert Position.cache_get(name, v, "stream-a", []) == :error
    end
  end

  describe "read_from_repo/4" do
    test "returns the committed position", %{name: name, version: v} do
      write_position(name, v, "stream-a", 9)

      assert Position.read_from_repo(Repo, name, v, "stream-a") == 9
    end

    test "returns nil for a stream with no row", %{name: name, version: v} do
      assert Position.read_from_repo(Repo, name, v, "stream-a") == nil
    end

    test "does not touch the cache", %{name: name, version: v} do
      write_position(name, v, "stream-a", 9)

      assert Position.read_from_repo(Repo, name, v, "stream-a") == 9
      assert Position.cache_get(name, v, "stream-a") == :error
    end
  end

  ## Helpers

  defp write_position(name, version, stream_id, position) do
    {:ok, _} =
      Ecto.Multi.new()
      |> Position.multi(name, version, stream_id, position)
      |> Repo.transaction()

    # multi/5 writes Postgres only; the caller updates the cache separately,
    # and these tests are about what happens when it has not.
    Position.drop_cache(name, version)

    :ok
  end

  # :telemetry handlers are node-global, so every projection alive on the node
  # emits into this one. Forwarding unconditionally would deliver another
  # test's cache_initialized event into this test's mailbox; the handler
  # filters on the projection name this test owns.
  defp attach_cache_telemetry(name) do
    ref = make_ref()
    test_pid = self()
    handler_id = {:position_repo_cache, ref}

    :telemetry.attach(
      handler_id,
      [:scriba, :projection, :cache_initialized],
      fn _event, measurements, %{name: emitted} = metadata, _ ->
        if emitted == name, do: send(test_pid, {ref, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    ref
  end
end
