defmodule ScribaBench.StragglerProjection do
  @moduledoc """
  A projection with one deliberately slow stream, for the ack-window test.

  Events on streams whose id starts with `"slow-"` block in the handler for
  `@straggler_delay_ms`; everything else commits immediately. That produces
  the shape the test needs: one event still inside a handler while later
  events from other streams commit, batch and acknowledge.

  `parallelism: 4` matters — with a single processor the straggler would
  block everything behind it and there would be no later ack to overtake it.
  """

  use Scriba.Projection,
    name: "ackloss",
    source: {Scriba.Source.Commanded, application: ScribaBench.CommandedApp},
    target: {Scriba.Target.Ecto, repo: ScribaBench.Repo},
    parallelism: 4

  alias ScribaBench.Events.Ticked
  alias ScribaBench.ReadModel

  @straggler_delay_ms 15_000

  def handle(%Ticked{stream: "slow-" <> _ = stream, n: n}, %{position: pos}) do
    Process.sleep(@straggler_delay_ms)
    {:insert, %ReadModel{stream: stream, n: n, position: pos}}
  end

  def handle(%Ticked{stream: stream, n: n}, %{position: pos}) do
    {:insert, %ReadModel{stream: stream, n: n, position: pos}}
  end

  def handle(_event, _meta), do: :skip

  def straggler_delay_ms, do: @straggler_delay_ms
end
