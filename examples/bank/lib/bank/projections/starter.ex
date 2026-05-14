defmodule Bank.Projections.Starter do
  @moduledoc """
  Starts the bank's Scriba projections at application boot.

  `Scriba.start_projection/1` isn't a child spec — it's a function call
  that adds a child to Scriba's own DynamicSupervisor. We wrap that
  call in a Task so it can live in the Bank.Application children list
  and run after `Bank.Repo` and `Bank.CommandedApp` are up.

  Idempotent: `Scriba.start_projection/1` returns `{:error, :already_started}`
  on a second invocation, which we treat as success. This matters for
  test scenarios and for Application restarts during development.
  """

  use Task, restart: :transient

  def start_link(_arg) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run do
    case Scriba.start_projection(Bank.Projections.Balances) do
      {:ok, _pid} -> :ok
      {:error, :already_started} -> :ok
      {:error, reason} -> raise "Bank projection failed to start: #{inspect(reason)}"
    end
  end
end
