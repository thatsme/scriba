defmodule Bank.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Bank.Repo,
      Bank.CommandedApp,
      # Starts the Scriba projection AFTER Repo + CommandedApp are up.
      # See Bank.Projections.Starter for the pattern.
      Bank.Projections.Starter
    ]

    opts = [strategy: :one_for_one, name: Bank.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
