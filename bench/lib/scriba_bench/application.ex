defmodule ScribaBench.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    # ScribaBench.EventStore is NOT listed here: the Commanded EventStore
    # adapter starts it as part of ScribaBench.CommandedApp. Starting it
    # here too fails with `already started`.
    children = [
      ScribaBench.Repo,
      ScribaBench.CommandedApp
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ScribaBench.Supervisor)
  end
end
