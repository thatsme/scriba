defmodule Scriba.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    # Shared ETS cache for projection positions. Created once here, owned by
    # this supervisor process, lives for the application's lifetime. See
    # `Scriba.Position` moduledoc for why this is shared rather than
    # per-projection.
    Scriba.Position.create_shared_table()

    children = [
      # Public addresses: coordinators, pipelines, per-projection supervisors.
      Scriba.Registry,
      # Broadway-internal addresses (producers, processors, batchers,
      # terminator). Separate Registry so the public one stays uncluttered.
      # Projection identity is in the key tuple — no atom interpolation,
      # so the BEAM atom table stays bounded regardless of projection count.
      Supervisor.child_spec(
        {Registry, keys: :unique, name: Scriba.Internals.Registry},
        id: Scriba.Internals.Registry
      ),
      # Placeholder slot for default telemetry handlers. v0.1: no attaches —
      # users wire their own. The slot exists so v0.2 dashboard work can
      # land into a stable supervision shape. See `Scriba.Telemetry.Handler`.
      Scriba.Telemetry.Handler,
      Scriba.Projections.Supervisor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
