defmodule Scriba.Telemetry.Handler do
  @moduledoc """
  Placeholder for default telemetry handlers attached by Scriba itself.

  ## v0.1 behavior — none

  This module is a supervised process slot. `init/1` returns `{:ok, %{}}`
  and no `:telemetry.attach_many/4` is called. Scriba emits the four v0.1
  events from `Scriba.Projection.Pipeline`:

    * `[:scriba, :projection, :event, :start | :stop | :exception]`
    * `[:scriba, :projection, :batch, :stop]`

  …and leaves attachment to user code. Application authors attach their
  own handlers (typically in their app's `start/2`), which keeps Scriba
  from competing with their observability stack.

  The supervisor child slot exists for v0.2 dashboard work to land into —
  the eventual home for default logging at `:exception` level and
  telemetry-pump behaviour.
  """

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts), do: {:ok, %{}}
end
