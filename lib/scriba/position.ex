defmodule Scriba.Position do
  @moduledoc """
  Per-stream position tracking helpers — pure functions, no process.

  Position lives in two places:

    * **Postgres** (`scriba_positions` table) — authoritative. Updated
      atomically with read-model writes inside the target's `Ecto.Multi`
      via `multi/5`. Per-stream rows: PK `(projection_name,
      projection_version, stream_id)`.
    * **ETS** (one **shared** named table for the whole BEAM, keyed on
      `{name, version, stream_id}` tuples) — hot-read cache for
      `Scriba.info/1`. **Not authoritative**.

  ## Shared-table model

  The cache lives in a single named ETS table — `Scriba.Position.Cache` — that
  is created once by `Scriba.Supervisor.init/1` and lives for the
  application's lifetime. Each cached entry is keyed by a
  `{name, version, stream_id}` tuple; `init_cache/3` and `drop_cache/2`
  scope all their work to one projection's rows in that shared table.

  This replaces the earlier per-projection `:named_table` design, which
  generated a fresh atom per projection — unbounded growth of the BEAM
  atom table when projection names were not bounded (e.g. property tests
  with `:erlang.unique_integer/1`-derived names). The shared table allocates
  exactly **one** atom regardless of projection count.

  No `PositionStore` GenServer mediates the cache. Workers (the Pipeline)
  write directly to the shared table via `cache_put/4`. Readers
  (`Scriba.info/2`, telemetry) read directly via `cache_get/3` /
  `stream_positions/2` / `safe_position/2`.

  ## Lifecycle

    * `Scriba.Supervisor.init/1` calls `create_shared_table/0` once at boot.
    * `Coordinator` on entering `:running` calls `init_cache/3` —
      **wipe-then-preload**: deletes any stale rows for this projection
      (from a previously-crashed Coordinator), then preloads from Postgres
      if `:repo` is set. Idempotent across pause→resume cycles.
    * `Coordinator` on entering `:stopped` calls `drop_cache/2` to clean up
      this projection's rows. (Crash recovery is covered by the wipe in
      the next `init_cache/3`; `:stopped` cleanup matters for projections
      that the user explicitly stops permanently.)

  ## Why no schema module

  We use raw SQL through `Ecto.Adapters.SQL.query/3` rather than an
  `Ecto.Schema`. The position rows are internal plumbing — there's no
  business logic that needs a changeset, and avoiding a schema means Scriba
  doesn't impose primary-key or timestamp conventions on the user.
  """

  alias Ecto.Adapters.SQL

  @type name :: String.t()
  @type version :: pos_integer()
  @type stream_id :: String.t()
  @type position :: non_neg_integer()

  # Single shared cache table. One atom for the whole BEAM, allocated at
  # module compile time.
  @cache_table __MODULE__.Cache

  # Cap on the rows preloaded from Postgres at init_cache time. Beyond this,
  # un-preloaded streams fall back to lazy lookup via `cache_get/4` with
  # `repo:`.
  @preload_cap 10_000

  ## Shared cache table

  @doc """
  Returns the atom name of the shared ETS cache table. Useful for
  introspection (`:ets.info/1`, `:observer`).
  """
  @spec cache_table() :: atom()
  def cache_table, do: @cache_table

  @doc """
  Creates the shared ETS cache table. Called once from
  `Scriba.Supervisor.init/1`. Idempotent — safe to call when the table
  already exists (returns `:ok` either way).

  The supervisor process owns the table; it dies with the application.
  """
  @spec create_shared_table() :: :ok
  def create_shared_table do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ets.new(@cache_table, [
          :set,
          :public,
          :named_table,
          {:write_concurrency, true},
          {:read_concurrency, true},
          {:decentralized_counters, true}
        ])

        :ok

      _ ->
        :ok
    end
  end

  ## Per-projection lifecycle

  @doc """
  Wipes any existing cache entries for `(name, version)` and (optionally)
  preloads up to #{@preload_cap} rows from Postgres into the shared table.

  Pass `repo: SomeRepo` to enable Postgres preload. Beyond the
  `@preload_cap` cutoff, un-preloaded streams fall back to lazy lookup via
  `cache_get/4` with `repo:`.

  Returns `:ok`.

  Emits `[:scriba, :projection, :cache_initialized]` telemetry with
  measurements `%{wiped_count, preloaded_count}` and metadata
  `%{name, version, source}` where `source` is `:postgres` (preloaded) or
  `:empty` (no repo configured).
  """
  @spec init_cache(name(), version(), keyword()) :: :ok
  def init_cache(name, version, opts \\ []) do
    wiped = drop_cache(name, version)

    {source, preloaded} =
      case Keyword.get(opts, :repo) do
        nil ->
          {:empty, 0}

        repo ->
          n = preload_from_repo(repo, name, version)
          {:postgres, n}
      end

    :telemetry.execute(
      [:scriba, :projection, :cache_initialized],
      %{wiped_count: wiped, preloaded_count: preloaded},
      %{name: name, version: version, source: source}
    )

    :ok
  end

  defp preload_from_repo(repo, name, version) do
    %{rows: rows} =
      SQL.query!(
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

    Enum.each(rows, fn [sid, pos] ->
      :ets.insert(@cache_table, {{name, version, sid}, pos})
    end)

    length(rows)
  end

  @doc """
  Removes all cache entries belonging to `(name, version)` from the shared
  table. Returns the number of rows deleted.

  No-op (returns 0) if the shared table doesn't exist yet — this can happen
  in unit-tested code paths that bypass the application supervisor.
  """
  @spec drop_cache(name(), version()) :: non_neg_integer()
  def drop_cache(name, version) do
    case :ets.whereis(@cache_table) do
      :undefined ->
        0

      _ ->
        match_spec = [{{{name, version, :_}, :_}, [], [true]}]
        :ets.select_delete(@cache_table, match_spec)
    end
  end

  ## Reads / writes

  @doc """
  Returns `{:ok, position}` for one stream from the ETS cache, or `:error`
  on cache miss.

  Pure ETS lookup — no I/O. If you want a cache-then-Postgres lookup that
  also backfills the cache on miss, use `cache_get/4` with `repo:`.
  """
  @spec cache_get(name(), version(), stream_id()) :: {:ok, position()} | :error
  def cache_get(name, version, stream_id) do
    cache_get(name, version, stream_id, [])
  end

  @doc """
  Cache-first lookup with optional Postgres fallback.

  Behaviour:

    * Hit in ETS → `{:ok, position}` (no Postgres call).
    * Miss in ETS, no `:repo` opt → `:error`.
    * Miss in ETS, `:repo` opt set → reads from Postgres via
      `read_from_repo/4`. If the row exists, the cache is back-filled and
      `{:ok, position}` is returned. If the row does not exist (no commit
      for this stream yet), returns `:error`.

  Source-side dedup is the primary caller of the
  `:repo`-backed form: dedup needs the durable cursor for a stream that
  hasn't appeared in the cache yet (e.g. a newly-discovered stream after
  Coordinator restart with a cache preloaded only up to `@preload_cap`
  rows).
  """
  @spec cache_get(name(), version(), stream_id(), keyword()) ::
          {:ok, position()} | :error
  def cache_get(name, version, stream_id, opts) do
    case :ets.whereis(@cache_table) do
      :undefined ->
        lazy_load(opts, name, version, stream_id)

      _ ->
        case :ets.lookup(@cache_table, {name, version, stream_id}) do
          [{_key, pos}] -> {:ok, pos}
          [] -> lazy_load(opts, name, version, stream_id)
        end
    end
  end

  defp lazy_load(opts, name, version, stream_id) do
    case Keyword.get(opts, :repo) do
      nil ->
        :error

      repo ->
        case read_from_repo(repo, name, version, stream_id) do
          nil ->
            :error

          pos ->
            cache_put(name, version, stream_id, pos)
            {:ok, pos}
        end
    end
  end

  @doc """
  Writes one stream's position into the shared cache. Silently no-ops if
  the shared table doesn't exist (path used by unit-test code that bypasses
  the supervisor).
  """
  @spec cache_put(name(), version(), stream_id(), position()) :: :ok
  def cache_put(name, version, stream_id, position) do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ok

      _ ->
        :ets.insert(@cache_table, {{name, version, stream_id}, position})
        :ok
    end
  end

  @doc """
  Returns a `%{stream_id => position}` map of all streams known to the
  cache for this projection. Empty map if the shared table doesn't exist
  or no streams are tracked.
  """
  @spec stream_positions(name(), version()) :: %{stream_id() => position()}
  def stream_positions(name, version) do
    case :ets.whereis(@cache_table) do
      :undefined ->
        %{}

      _ ->
        @cache_table
        |> :ets.match_object({{name, version, :_}, :_})
        |> Map.new(fn {{_n, _v, sid}, pos} -> {sid, pos} end)
    end
  end

  @doc """
  Returns the minimum committed position across the streams this projection
  currently has **in cache**. Introspection only — surfaced through
  `Scriba.info/2` as a rough "how far behind is the laggard" number.

  Returns 0 when the projection has no cached streams (cache was just
  initialized, or the table doesn't exist).

  ## This is not a replay point

  An earlier version of this docstring called it a "safe replay point" and
  claimed a replica resuming here could not miss an event. That is false in
  two independent ways, both of which push the result **too high** — the
  direction that skips events:

    * **The cache is a capped subset.** `init_cache/3` preloads at most
      #{@preload_cap} rows, ordered by `stream_id`. A minimum taken over a
      subset is greater than or equal to the minimum over the whole set.

    * **Untouched streams are invisible.** Only streams this projection has
      actually written to have rows at all. A projection that handles a
      narrow slice of event types — the shape the `:all` subscription plus a
      `:skip` catch-all produces — has no row for most streams in the store,
      and they contribute nothing to the minimum.

  It is only equal to a true replay point when every stream in the event
  store has a committed row and there are fewer than #{@preload_cap} of them.
  Do not build resume-from-here on top of this. When v0.3 needs a real replay
  point it should be an uncapped `MIN(position)` aggregate against Postgres,
  which the `(projection_name, projection_version)` index already supports.
  """
  @spec safe_position(name(), version()) :: position()
  def safe_position(name, version) do
    case stream_positions(name, version) do
      empty when map_size(empty) == 0 -> 0
      positions -> positions |> Map.values() |> Enum.min()
    end
  end

  ## Repo resolution (shared by Coordinator + Pipeline)

  @doc """
  Resolves the `Ecto.Repo` to use for position-tracking I/O — both the
  Coordinator's `init_cache/3` preload and the Pipeline's source-side
  dedup `cache_get/4` fallback consult this.

  Single source of truth: keeping repo resolution in one place means the
  Coordinator and Pipeline can never disagree about which repo backs the
  cache. Diverging would silently break source-side dedup (Pipeline reads
  one repo, Coordinator writes another).

  Order:

    1. Explicit `:repo` opt on the projection wins (override path; e.g. a
       custom target that also wants cache preload from Postgres).
    2. `Scriba.Target.Ecto`'s target_spec carries `:repo` in its target
       opts — extract it. Raises if missing, since the Ecto target needs
       it anyway.
    3. Otherwise `nil` (Test target, custom non-Postgres targets).
  """
  @spec resolve_repo(keyword(), {module(), keyword()}) :: module() | nil
  def resolve_repo(opts, target_spec) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} -> repo
      :error -> repo_from_target_spec(target_spec)
    end
  end

  defp repo_from_target_spec({Scriba.Target.Ecto, target_opts}) when is_list(target_opts) do
    Keyword.fetch!(target_opts, :repo)
  end

  defp repo_from_target_spec(_), do: nil

  ## Postgres (authoritative)

  @doc """
  Reads one stream's position from Postgres. Returns `nil` if no row exists
  for this `(name, version, stream_id)`.
  """
  @spec read_from_repo(module(), name(), version(), stream_id()) ::
          position() | nil
  def read_from_repo(repo, name, version, stream_id) do
    case SQL.query!(
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
  inside an `c:Scriba.Target.apply_batch/6` implementation building the
  atomic-commit Multi alongside its handler operations.

  The Multi step is keyed by `{:scriba_position, stream_id}` so a batch
  touching N streams produces N independent steps.

  ## Monotonicity is enforced here, not upstream

  The upsert is `GREATEST(existing, incoming)`, so a cursor can never move
  backwards no matter what the caller passes.

  The Pipeline also filters `:skip` results out of `stream_advances` so a
  redelivered below-cursor batch does not regress the cursor. That filter is
  still correct and still wanted — but it is one `Enum.reject/2` in another
  module, and a cursor moving backwards is this library's worst failure mode:
  it silently re-applies committed effects. The invariant belongs in the
  storage layer where no upstream change can violate it.

  Trade-off, deliberately taken: `GREATEST` also masks a genuine Pipeline bug
  that computes a regressing advance, converting loud corruption into a silent
  no-op. That is the right trade for a v0.1 whose stated first principle is
  correctness over throughput. If detection is later wanted, add
  `WHERE EXCLUDED.position > scriba_positions.position` and raise on zero rows
  affected — but do not go back to an unconditional `SET`.
  """
  @spec multi(Ecto.Multi.t(), name(), version(), stream_id(), position()) ::
          Ecto.Multi.t()
  def multi(multi, name, version, stream_id, new_position) do
    Ecto.Multi.run(multi, {:scriba_position, stream_id}, fn repo, _changes ->
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      SQL.query(
        repo,
        """
        INSERT INTO scriba_positions
          (projection_name, projection_version, stream_id, position, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (projection_name, projection_version, stream_id)
        DO UPDATE SET position = GREATEST(scriba_positions.position, EXCLUDED.position),
                      updated_at = EXCLUDED.updated_at
        """,
        [name, version, stream_id, new_position, now]
      )
    end)
  end
end
