defmodule Scriba.Property.CrashRecoveryTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test.Generators
  alias Scriba.Test.PropertyHelpers, as: H

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  property "P3: every input event appears in the deduped commit log after recovery" do
    check all events <- Generators.events_gen(min: 1, max: 30),
              crashes <- Generators.crash_delays_gen(max: 3),
              max_runs: 1000 do
      {:ok, agent} = TestTarget.start_link()

      try do
        name = "p3-#{:erlang.unique_integer([:positive])}"
        opts = H.projection_opts(events, agent, name)
        {:ok, initial_sup} = H.start_projection(opts)

        # Run the crash schedule, restarting after each kill.
        final_sup =
          Enum.reduce(crashes, initial_sup, fn delay_ms, current_sup ->
            Process.sleep(delay_ms)
            H.kill_and_wait(current_sup)
            {:ok, new_sup} = H.start_projection(opts)
            new_sup
          end)

        try do
          # After the last crash + restart, the Test source replays everything;
          # the deduped log must converge to the input set.
          case H.wait_until_unique(agent, length(events), 10_000) do
            {:ok, _} -> :ok
            {:error, :timeout, got, expected} ->
              flunk(
                "P3 timed out: only #{got}/#{expected} unique event_ids " <>
                  "after #{length(crashes)} crash(es). " <>
                  "events=#{inspect(Enum.map(events, & &1.id))}"
              )
          end

          actual_ids = TestTarget.commits(agent) |> H.unique_event_ids()
          expected_ids = events |> Enum.map(& &1.id) |> Enum.sort()

          missing = expected_ids -- actual_ids
          extra = actual_ids -- expected_ids

          assert actual_ids == expected_ids,
                 "P3 violated: missing=#{inspect(missing)} extra=#{inspect(extra)}"
        after
          H.stop_projection(final_sup)
        end
      after
        Agent.stop(agent)
      end
    end
  end
end
