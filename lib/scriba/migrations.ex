defmodule Scriba.Migrations do
  @moduledoc """
  Ecto migration helpers users invoke from their own migration files.

  ## A fresh install

      defmodule MyApp.Repo.Migrations.AddScribaTables do
        use Ecto.Migration

        def up, do: Scriba.Migrations.up()
        def down, do: Scriba.Migrations.down()
      end

  `up/1` brings the schema to the latest version: `scriba_positions`
  (per-stream cursors), `scriba_dead_letters` (§9) and `scriba_watermarks`
  (the contiguous global position per projection).

  ## Upgrading an existing install

  Schema changes ship as numbered steps, and `:from` says which one you
  already have. A projection running Scriba 0.1.x has version 1, so:

      defmodule MyApp.Repo.Migrations.UpgradeScriba do
        use Ecto.Migration

        def up, do: Scriba.Migrations.up(from: 1)
        def down, do: Scriba.Migrations.down(to: 1)
      end

  Which steps exist:

  | Version | Adds |
  |---|---|
  | 1 | `scriba_positions`, `scriba_dead_letters` |
  | 2 | `scriba_watermarks` |

  Scriba tracks no migration state of its own — the version lives in your
  migration files, where Ecto already records what ran. That is why `:from`
  is explicit rather than detected: guessing wrong would either skip a step
  or re-run one, and your migration file is the thing that knows.

  All tables are owned by the user's repo.

  ## stream_id constraint

  `stream_id` is `varchar(255)`. Adapters whose native stream identifiers are
  richer (UUIDs, integers, composite keys) must convert to a string at the
  boundary. This is a deliberate v0.1 simplification — it lets the position
  cursor schema and ETS keys share one type without per-adapter generics.
  """

  use Ecto.Migration

  @latest 2

  @doc "The newest schema version this release knows about."
  @spec latest_version() :: pos_integer()
  def latest_version, do: @latest

  @doc """
  Applies every schema step above `:from` (default `0`) up to `:to`
  (default: the latest).
  """
  @spec up(keyword()) :: :ok
  def up(opts \\ []) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, @latest)

    validate!(from, to)

    for version <- (from + 1)..to//1, do: step_up(version)

    :ok
  end

  @doc """
  Reverses every schema step from `:from` (default: the latest) down to
  `:to` (default `0`, i.e. everything).
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) do
    from = Keyword.get(opts, :from, @latest)
    to = Keyword.get(opts, :to, 0)

    validate!(to, from)

    for version <- from..(to + 1)//-1, do: step_down(version)

    :ok
  end

  defp validate!(lower, upper) do
    cond do
      lower < 0 or upper > @latest ->
        raise ArgumentError,
              "Scriba schema versions run from 0 to #{@latest}; got #{lower}..#{upper}"

      lower > upper ->
        raise ArgumentError,
              "nothing to do: #{lower} is already at or above #{upper}"

      true ->
        :ok
    end
  end

  ## Version 1 — cursors and dead letters

  defp step_up(1) do
    # Per-stream position cursor. One row per
    # (projection_name, projection_version, stream_id). The pipeline updates
    # the row whose stream_id equals the event's stream_id; cross-partition
    # commit ordering can no longer cause silent drops because each stream
    # has its own cursor.
    create table(:scriba_positions, primary_key: false) do
      add :projection_name, :string, size: 255, null: false, primary_key: true
      add :projection_version, :integer, null: false, primary_key: true
      add :stream_id, :string, size: 255, null: false, primary_key: true
      add :position, :bigint, null: false
      add :updated_at, :utc_datetime_usec, null: false
    end

    # Aggregate lookups across all streams of one projection need this index.
    create index(:scriba_positions, [:projection_name, :projection_version])

    create table(:scriba_dead_letters) do
      add :projection_name, :string, size: 255, null: false
      add :projection_version, :integer, null: false
      add :position, :bigint, null: false
      add :stream_id, :string, size: 255
      add :event_type, :string, size: 255
      add :event_data, :map, null: false
      add :error_kind, :string, size: 64, null: false
      add :error_message, :text
      add :error_stacktrace, :text
      add :occurred_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create index(:scriba_dead_letters, [:projection_name, :projection_version])
    create index(:scriba_dead_letters, [:occurred_at])
  end

  ## Version 2 — the contiguous watermark

  defp step_up(2) do
    # One row per projection, unlike scriba_positions. It answers the
    # question per-stream cursors cannot: how far has this projection got
    # *overall*, counting nothing it has not yet applied. See
    # `Scriba.Watermark`.
    create table(:scriba_watermarks, primary_key: false) do
      add :projection_name, :string, size: 255, null: false, primary_key: true
      add :projection_version, :integer, null: false, primary_key: true
      add :position, :bigint, null: false
      add :occurred_at, :utc_datetime_usec
      add :updated_at, :utc_datetime_usec, null: false
    end
  end

  defp step_down(2), do: drop(table(:scriba_watermarks))

  defp step_down(1) do
    drop table(:scriba_dead_letters)
    drop table(:scriba_positions)
  end
end
