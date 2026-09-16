defmodule Bank.Repo.Migrations.InstallScriba do
  @moduledoc """
  Delegates to `Scriba.Migrations` for Scriba's own tables. Same pattern
  documented at the top of `Scriba.Migrations` — real users copy this exact
  shape.

  `up/0` brings the schema to the latest version, so a database created today
  gets all three tables from this one migration. A database created before
  `scriba_watermarks` existed gets it from the next migration instead, which
  is the upgrade path an existing user takes.
  """

  use Ecto.Migration

  def up, do: Scriba.Migrations.up()
  def down, do: Scriba.Migrations.down()
end
