defmodule Scriba.Test.Migrations.CreateScribaTables do
  @moduledoc """
  Test-suite migration that creates `scriba_positions` and
  `scriba_dead_letters` by delegating to `Scriba.Migrations`.

  This module exists because `Ecto.Migrator.run/4` expects a list of
  proper migration modules (each `use Ecto.Migration` and registered in
  `schema_migrations`), not the helper module users invoke from inside
  their own migration files. Delegating to `Scriba.Migrations.up/0` here
  exercises the exact integration boundary real users hit.
  """

  use Ecto.Migration

  def up, do: Scriba.Migrations.up()
  def down, do: Scriba.Migrations.down()
end
