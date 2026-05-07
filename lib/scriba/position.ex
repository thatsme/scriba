defmodule Scriba.Position do
  @moduledoc """
  Per-stream position tracking helpers — pure functions, no process.

  Position lives in two places:

    * **Postgres** (`scriba_positions` table) — authoritative. Updated
      atomically with read-model writes inside the target's `Ecto.Multi`
      via `multi/5`. Per-stream rows: PK `(projection_name,
      projection_version, stream_id)`.
    * **ETS** (one table per projection, `:public + :named_table`) — hot-read
      cache for `Scriba.info/1`. **Not authoritative**.

  No `PositionStore` GenServer mediates either. The Coordinator creates the
  ETS table on entering `:running`. The Pipeline writes one cache entry per
  stream per batch via `cache_put/4`.

  ## TODO

  Per-projection named ETS tables generate atoms unboundedly when projection
  names are not bounded (e.g. property tests with `:erlang.unique_integer/1`).
  collapses this into a single shared ETS table keyed on
  `{name, version, stream_id}`. Until then, `init_cache/3` and `drop_cache/2`
  manage per-projection tables.

  ## Why no schema module

  We use raw SQL through `Ecto.Adapters.SQL.query/3` rather than an
  `Ecto.Schema`. The position rows are internal plumbing — there's no
  business logic that needs a changeset, and avoiding a schema means Scriba
  doesn't impose primary-key or timestamp conventions on the user.
  """

  @type name :: String.t()
  @type version :: pos_integer()
  @type stream_id :: String.t()
  @type position :: non_neg_integer()

  # Cap on the rows preloaded from Postgres at init_cache time. Beyond this,
  # missing entries fall back to a lazy lookup from Postgres on cache_get
  # miss. Trade-off documented in 
  @preload_cap 10_000

  ## ETS cache

  @doc "Returns the atom name of the per-projection ETS cache table."
  @spec table_name(name(), version()) :: atom()
  def table_name(name, version) do
    String.to_atom("scriba.position.#{name}.v#{version}")
  end

  @doc """
  Creates the per-projection ETS cache. Idempotent: a no-op if the table
  already exists.

  Pass `repo: SomeRepo` to preload up to #{@preload_cap} `(stream_id, position)`
  rows from Postgres. Beyond the cap, callers should rely on `cache_get/3`
  falling back to `read_from_repo/4` on miss (not yet implemented; tracked
  in the cache itself returning `:error` for now).

  Emits `[:scriba, :projection, :cache_initialized]` telemetry with metadata
  `%{name, version, source, stream_count}` where `source` is `:postgres`
  (preloaded) or `:empty` (no repo configured).
  """
  @spec init_cache(name(), version(), keyword()) :: atom()
  def init_cache(name, version, opts \\ []) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined ->
        :ets.new(table, [
          :set,
          :public,
          :named_table,
          {:write_concurrency, true},
          {:read_concurrency, true},
          {:decentralized_counters, true}
        ])

        {source, count} =
          case Keyword.get(opts, :repo) do
            nil ->
              {:empty, 0}

            repo ->
              n = preload_from_repo(table, repo, name, version)
              {:postgres, n}
          end

        :telemetry.execute(
          [:scriba, :projection, :cache_initialized],
          %{stream_count: count},
          %{name: name, version: version, source: source}
        )

      _ ->
        :ok
    end

    table
  end

  defp preload_from_repo(table, repo, name, version) do
    %{rows: rows} =
      Ecto.Adapters.SQL.query!(
        repo,
        """
        SELECT stream_id, position
          FROM scriba_positions
         WHERE projection_name = $1
           AND projection_version = $2
         ORDER BY stream_id
         LIMIT $3
        """,
        [name, version, @preload_cap]
      )

    Enum.each(rows, fn [sid, pos] -> :ets.insert(table, {sid, pos}) end)

    length(rows)
  end

  @doc "Removes the per-projection ETS cache. Used on Coordinator stop."
  @spec drop_cache(name(), version()) :: :ok
  def drop_cache(name, version) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined -> :ok
      _ -> :ets.delete(table) && :ok
    end
  end

  @doc """
  Returns `{:ok, position}` for one stream from the ETS cache, or `:error`
  on cache miss. Does **not** fall back to Postgres — callers wanting
  durable lookup should call `read_from_repo/4` explicitly on miss.
  """
  @spec cache_get(name(), version(), stream_id()) :: {:ok, position()} | :error
  def cache_get(name, version, stream_id) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(table, stream_id) do
          [{_sid, pos}] -> {:ok, pos}
          [] -> :error
        end
    end
  end

  @doc """
  Writes one stream's position into the ETS cache. Silently no-ops if the
  cache table does not exist (e.g. the projection isn't running, or the
  Coordinator is mid-restart).
  """
  @spec cache_put(name(), version(), stream_id(), position()) :: :ok
  def cache_put(name, version, stream_id, position) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined -> :ok
      _ -> :ets.insert(table, {stream_id, position}) && :ok
    end
  end

  @doc """
  Returns a `%{stream_id => position}` map of all streams known to the cache
  for this projection. Empty map if the cache table doesn't exist.
  """
  @spec stream_positions(name(), version()) :: %{stream_id() => position()}
  def stream_positions(name, version) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined -> %{}
      _ -> :ets.tab2list(table) |> Map.new()
    end
  end

  @doc """
  Returns the **safe replay point**: the minimum position across all
  streams in the cache. A new replica resuming from this position is
  guaranteed not to miss any event in any stream.

  Returns 0 when the cache is empty (no streams have committed yet).
  """
  @spec safe_position(name(), version()) :: position()
  def safe_position(name, version) do
    table = table_name(name, version)

    case :ets.whereis(table) do
      :undefined ->
        0

      _ ->
        case :ets.tab2list(table) do
          [] -> 0
          rows -> rows |> Enum.map(fn {_sid, pos} -> pos end) |> Enum.min()
        end
    end
  end

  ## Postgres (authoritative)

  @doc """
  Reads one stream's position from Postgres. Returns `nil` if no row exists
  for this `(name, version, stream_id)`.
  """
  @spec read_from_repo(module(), name(), version(), stream_id()) ::
          position() | nil
  def read_from_repo(repo, name, version, stream_id) do
    case Ecto.Adapters.SQL.query!(
           repo,
           """
           SELECT position
             FROM scriba_positions
            WHERE projection_name = $1
              AND projection_version = $2
              AND stream_id = $3
           """,
           [name, version, stream_id]
         ) do
      %{rows: [[pos]]} -> pos
      %{rows: []} -> nil
    end
  end

  @doc """
  Appends a per-stream position upsert to an `Ecto.Multi`. Use this from
  inside an `apply_batch/5` implementation building the atomic-commit Multi
  alongside its handler operations.

  The Multi step is keyed by `{:scriba_position, stream_id}` so a batch
  touching N streams produces N independent steps.
  """
  @spec multi(Ecto.Multi.t(), name(), version(), stream_id(), position()) ::
          Ecto.Multi.t()
  def multi(multi, name, version, stream_id, new_position) do
    Ecto.Multi.run(multi, {:scriba_position, stream_id}, fn repo, _changes ->
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      Ecto.Adapters.SQL.query(
        repo,
        """
        INSERT INTO scriba_positions
          (projection_name, projection_version, stream_id, position, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (projection_name, projection_version, stream_id)
        DO UPDATE SET position = EXCLUDED.position, updated_at = EXCLUDED.updated_at
        """,
        [name, version, stream_id, new_position, now]
      )
    end)
  end
end
