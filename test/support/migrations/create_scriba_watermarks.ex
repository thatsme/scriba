defmodule Scriba.Test.Migrations.CreateScribaWatermarks do
  @moduledoc """
  Test-suite migration that adds `scriba_watermarks` to a database already at
  schema version 1.

  It exercises the upgrade path rather than the fresh-install one: a database
  created before this table existed is exactly what an existing user has, and
  `Scriba.Migrations.up(from: 1)` is what they write. A fresh install goes
  through `Scriba.Migrations.up/0` in
  `Scriba.Test.Migrations.CreateScribaTables`, which now creates both
  versions in one step — so between them the two paths are both covered.
  """

  use Ecto.Migration

  def up, do: Scriba.Migrations.up(from: 1)
  def down, do: Scriba.Migrations.down(to: 1)
end
