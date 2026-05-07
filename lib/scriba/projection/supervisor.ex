defmodule Scriba.Projection.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)
    Supervisor.start_link(__MODULE__, opts, name: via_tuple(name, version))
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  def via_tuple(name, version) do
    {:via, Registry, {Scriba.Registry, {:projection_supervisor, name, version}}}
  end

  @impl Supervisor
  def init(opts) do
    coordinator_opts = Keyword.put(opts, :supervisor_pid, self())

    children = [
      {Scriba.Projection.Coordinator, coordinator_opts},
      {Scriba.Projection.Pipeline, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
