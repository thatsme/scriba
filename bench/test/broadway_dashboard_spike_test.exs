defmodule ScribaBench.BroadwayDashboardSpikeTest do
  @moduledoc """
  Does `broadway_dashboard` already work on Scriba's pipelines?

  The question is worth answering before building anything: a projection *is*
  a Broadway topology, and the ecosystem ships operational UIs as companion
  packages rather than inside the library (`oban_web`, `broadway_dashboard`
  itself). If the existing page already renders Scriba, then "a LiveView
  dashboard" is documentation, not a feature.

  Two things have to hold, and neither is obvious, because Scriba names its
  pipelines with `{:via, Registry, {Scriba.Registry, {:pipeline, name,
  version}}}` rather than a plain module atom:

    1. `Broadway.all_running/0` has to list them at all.
    2. `BroadwayDashboard.Metrics.listen/3` has to accept that name — it
       derives a server name from it and looks the process up by it.

  This is a spike kept as a test. It asserts the integration a README claim
  would rest on, so the claim cannot quietly stop being true.
  """

  use ExUnit.Case, async: false

  alias BroadwayDashboard.Metrics
  alias ScribaBench.CommandedApp

  setup do
    on_exit(fn ->
      for {_, pid, _, _} <- DynamicSupervisor.which_children(Scriba.Projections.Supervisor) do
        DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
      end
    end)

    :ok
  end

  test "broadway_dashboard discovers and attaches to a Scriba projection" do
    {:ok, _} =
      Scriba.start_projection(ScribaBench.Projection,
        source:
          {Scriba.Source.Commanded,
           application: CommandedApp,
           subscription_name: "dash-#{System.unique_integer([:positive])}",
           start_from: :current}
      )

    pipeline =
      wait_for(fn ->
        Enum.find(Broadway.all_running(), &scriba_pipeline?/1)
      end)

    assert pipeline, """
    Broadway.all_running/0 did not list the projection, so the dashboard has
    nothing to discover: #{inspect(Broadway.all_running())}
    """

    # What the dashboard does on mount once a pipeline is selected.
    result = Metrics.listen(node(), self(), pipeline)

    assert {:ok, payload} = result,
           "broadway_dashboard could not attach to #{inspect(pipeline)}: #{inspect(result)}"

    assert is_integer(payload.successful)
    assert is_integer(payload.failed)
    assert payload.topology_workload != nil

    # The topology it renders is the projection's real shape.
    topology = Broadway.topology(pipeline)
    assert Keyword.has_key?(topology, :processors)
    assert Keyword.has_key?(topology, :batchers)

    layers = BroadwayDashboard.PipelineGraph.build_layers(payload.topology_workload)

    IO.puts("""

    === broadway_dashboard spike ===
      pipeline name:   #{inspect(pipeline)}
      server name:     #{inspect(Metrics.server_name(pipeline))}
      topology:        #{inspect(Keyword.keys(topology))}
      processors:      #{inspect(topology[:processors])}
      batchers:        #{inspect(topology[:batchers])}
      graph layers:    #{length(layers)}
      counters:        successful=#{payload.successful} failed=#{payload.failed}

    Verdict: broadway_dashboard renders Scriba projections as-is. No dashboard
    needs building; it needs documenting.
    """)
  end

  defp scriba_pipeline?({:via, _registry, {Scriba.Registry, {:pipeline, _name, _version}}}),
    do: true

  defp scriba_pipeline?(_other), do: false

  defp wait_for(fun, attempts \\ 50) do
    case fun.() do
      nil when attempts > 0 ->
        Process.sleep(100)
        wait_for(fun, attempts - 1)

      result ->
        result
    end
  end
end
