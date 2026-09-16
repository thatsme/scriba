defmodule ScribaBench.Projection do
  @moduledoc """
  One insert per event, no queries, no computation. Whatever throughput
  this reaches is an upper bound for any real projection.

  `parallelism: 8` is deliberate: part of what the benchmark answers is
  whether parallelism has any effect at all against a real event store,
  or whether upstream delivery is the binding constraint.
  """

  use Scriba.Projection,
    name: "bench",
    source: {Scriba.Source.Commanded, application: ScribaBench.CommandedApp},
    target: {Scriba.Target.Ecto, repo: ScribaBench.Repo},
    parallelism: 8

  alias ScribaBench.Events.Ticked
  alias ScribaBench.ReadModel

  def handle(%Ticked{stream: stream, n: n}, %{position: pos}) do
    {:insert, %ReadModel{stream: stream, n: n, position: pos}}
  end

  def handle(_event, _meta), do: :skip
end
