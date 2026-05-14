defmodule Bank.Repo.Migrations.InstallScriba do
  @moduledoc """
  Delegates to `Scriba.Migrations` for the `scriba_positions` and
  `scriba_dead_letters` tables. Same pattern documented at the top of
  `Scriba.Migrations` — real users copy this exact shape.
  """

  use Ecto.Migration

  def up, do: Scriba.Migrations.up()
  def down, do: Scriba.Migrations.down()
end
