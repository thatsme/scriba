defmodule ScribaBench.Repo.Migrations.AddScribaWatermarks do
  use Ecto.Migration

  # The upgrade path: this database was created at schema version 1.
  def up, do: Scriba.Migrations.up(from: 1)
  def down, do: Scriba.Migrations.down(to: 1)
end
