defmodule Scriba.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    children = [
      Scriba.Registry,
      Scriba.Projections.Supervisor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
