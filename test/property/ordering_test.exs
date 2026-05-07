defmodule Scriba.Property.OrderingTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test.Generators
  alias Scriba.Test.PropertyHelpers, as: H

  @moduletag timeout: 600_000

  property "P1: per-stream events arrive at the target in source order" do
    check all events <- Generators.events_gen(), max_runs: 1000 do
      {:ok, agent} = TestTarget.start_link()

      try do
        name = "p1-#{:erlang.unique_integer([:positive])}"
        opts = H.projection_opts(events, agent, name)
        {:ok, sup} = H.start_projection(opts)

        try do
          assert {:ok, _} = H.wait_until_unique(agent, length(events), 5_000)

          id_to_stream = Map.new(events, fn e -> {e.id, e.stream_id} end)
          commits = TestTarget.commits(agent)

          # Per stream: positions in commit order must be non-decreasing
          # (handlers see same-stream events in source order).
          commits
          |> Enum.group_by(fn {id, _pos} -> Map.fetch!(id_to_stream, id) end)
          |> Enum.each(fn {stream_id, stream_commits} ->
            positions = Enum.map(stream_commits, fn {_id, p} -> p end)

            assert positions == Enum.sort(positions),
                   "stream #{stream_id} commits out of order: #{inspect(positions)}"
          end)
        after
          H.stop_projection(sup)
        end
      after
        Agent.stop(agent)
      end
    end
  end
end
