defmodule Scriba.Source do
  @moduledoc """
  Behaviour for event sources.

  A source pulls events from somewhere — a Commanded event store, an external
  queue, an HTTP feed — and yields them as `Broadway.Message` values whose
  `:data` is a `Scriba.Event`. The engine plugs the source into a Broadway
  pipeline as the producer.

  Implementations must:

    * Implement this behaviour's `child_spec/1` and `start_link/1` so the
      source can be placed under a supervisor.
    * Implement `Broadway.Producer` so events can be consumed by the
      pipeline.
  """

  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()
  @callback start_link(opts :: keyword()) :: GenServer.on_start()
end
