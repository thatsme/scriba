defmodule Scriba.Migrations do
  @moduledoc """
  Ecto migration helpers users invoke from their own migration files.

  ## Usage

      defmodule MyApp.Repo.Migrations.AddScribaTables do
        use Ecto.Migration

        def up, do: Scriba.Migrations.up()
        def down, do: Scriba.Migrations.down()
      end

  Creates `scriba_positions` (per-stream cursors)
  and `scriba_dead_letters` (§9). Both tables are owned by the user's repo —
  Scriba does not manage migrations on its own.

  ## stream_id constraint

  `stream_id` is `varchar(255)`. Adapters whose native stream identifiers are
  richer (UUIDs, integers, composite keys) must convert to a string at the
  boundary. This is a deliberate v0.1 simplification — it lets the position
  cursor schema and ETS keys share one type without per-adapter generics.
  """

  use Ecto.Migration

  def up do
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

    # Aggregate lookups across all streams of one projection (e.g. for
    # safe_position computation) need this index.
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

  def down do
    drop table(:scriba_dead_letters)
    drop table(:scriba_positions)
  end
end
