defmodule Bank.Repo.Migrations.AddScribaWatermarks do
  @moduledoc """
  The upgrade path in miniature: this database was created when Scriba's
  schema was at version 1, so it gets version 2 — `scriba_watermarks` — from
  its own migration rather than by re-running the first one.
  """

  use Ecto.Migration

  def up, do: Scriba.Migrations.up(from: 1)
  def down, do: Scriba.Migrations.down(to: 1)
end
