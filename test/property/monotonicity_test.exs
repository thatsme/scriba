defmodule Scriba.Property.MonotonicityTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test.Generators
  alias Scriba.Test.PropertyHelpers, as: H

  @moduletag timeout: 600_000

  property "P2: target's stored position is monotonically non-decreasing" do
    check all events <- Generators.events_gen(), max_runs: 1000 do
      {:ok, agent} = TestTarget.start_link()

      try do
        name = "p2-#{:erlang.unique_integer([:positive])}"
        opts = H.projection_opts(events, agent, name)
        {:ok, sup} = H.start_projection(opts)

        try do
          # Sample position frequently while events are flowing.
          samples = sample_positions(agent, 30, 5)

          assert {:ok, _} = H.wait_until_unique(agent, length(events), 5_000)
          final = TestTarget.position(agent)

          all_samples = samples ++ [final]

          assert all_samples == Enum.sort(all_samples),
                 "position regressed: #{inspect(all_samples)}"
        after
          H.stop_projection(sup)
        end
      after
        Agent.stop(agent)
      end
    end
  end

  defp sample_positions(_agent, 0, _interval), do: []

  defp sample_positions(agent, n, interval) do
    Process.sleep(interval)
    [TestTarget.position(agent) | sample_positions(agent, n - 1, interval)]
  end
end
