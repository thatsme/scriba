defmodule Scriba.Test.Source do
  @moduledoc false

  @behaviour Scriba.Source

  use GenStage

  @impl Scriba.Source
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @impl Scriba.Source
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    events = Keyword.get(opts, :events, [])
    {:producer, %{queue: events}}
  end

  @impl GenStage
  def handle_demand(demand, %{queue: queue} = state) when demand > 0 do
    {to_send, remaining} = Enum.split(queue, demand)
    messages = Enum.map(to_send, &to_message/1)
    {:noreply, messages, %{state | queue: remaining}}
  end

  defp to_message(%Scriba.Event{} = event) do
    %Broadway.Message{
      data: event,
      acknowledger: Broadway.NoopAcknowledger.init()
    }
  end
end
