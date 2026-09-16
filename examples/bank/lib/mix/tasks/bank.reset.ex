defmodule Mix.Tasks.Bank.Reset do
  @moduledoc """
  Truncates the read-model state so the next `mix bank.demo` starts
  clean.

  `account_balances` is the read model; `scriba_positions` is Scriba's
  cursor tracking. Both must be emptied together — leaving cursors
  intact would make the projection skip past the new events.

  The Commanded event store is in-memory (per config/config.exs), so
  it's empty on every BEAM start; no reset needed for the event side.
  """

  use Mix.Task

  @shortdoc "Truncate the bank demo's read-model tables"

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start", [])

    Bank.Repo.query!("TRUNCATE account_balances, scriba_positions, scriba_dead_letters, scriba_watermarks")

    Mix.shell().info("Bank demo state reset.")
  end
end
