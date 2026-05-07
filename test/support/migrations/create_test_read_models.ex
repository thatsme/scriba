defmodule Scriba.Test.Migrations.CreateTestReadModels do
  @moduledoc """
  Test-suite migration for the read-model table that property_db handlers
  insert into. Schema is defined in `Scriba.Test.ReadModel`.

  PK on `event_id` so duplicate inserts (same event_id) violate the
  primary-key constraint — that's how PD1 will detect double-applies under
  crash injection.
  """

  use Ecto.Migration

  def up do
    create table(:test_read_models, primary_key: false) do
      add :event_id, :string, size: 255, primary_key: true
      add :stream_id, :string, size: 255, null: false
      add :position, :bigint, null: false
    end

    create index(:test_read_models, [:stream_id])
  end

  def down do
    drop table(:test_read_models)
  end
end
