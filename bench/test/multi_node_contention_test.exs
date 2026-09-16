defmodule ScribaBench.MultiNodeContentionTest do
  @moduledoc """
  What happens to a healthy projection when another one cannot get the
  subscription it wants?

  This is the shape of a rolling deploy. A persistent subscription admits one
  subscriber, so on a three-node deploy one node wins the race and the others
  get `{:error, :subscription_already_exists}`. `Scriba.Source.Commanded`
  retries that five times over ~1.5s and then raises, and a raising producer
  is restarted by `Scriba.Projection.Supervisor` — which runs at 30 restarts
  per 60 seconds and, when that budget is exhausted, dies and propagates
  toward `Scriba.Projections.Supervisor` and the host application.

  The claim under test is therefore not "the loser fails" — it is supposed to
  fail. It is: **does the loser's failure damage the winner?**

  Measured here:

    * whether the losing projection restart-loops, and how fast
    * whether `Scriba.Projections.Supervisor` — shared by every projection in
      the VM — survives it
    * whether the winning projection keeps committing events throughout

  ## What this does not reproduce

  One BEAM, not three. The contention is faithful, because the subscription
  is held in the event store and shared by every node; the supervision
  blast radius is faithful, because each node runs the same tree. What it
  cannot show is a genuine failover — the winner dying and a standby taking
  over — since both projections here have distinct identities and the
  registry will not admit two with the same one.
  """

  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias ScribaBench.{CommandedApp, Repo, StragglerProjection}
  alias ScribaBench.Events.Ticked

  @observe_ms 25_000
  @sample_ms 250

  setup do
    reset_event_store()
    SQL.query!(Repo, "TRUNCATE bench_rows, scriba_positions, scriba_dead_letters", [])

    on_exit(fn ->
      for {_, pid, _, _} <- DynamicSupervisor.which_children(Scriba.Projections.Supervisor) do
        DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
      end
    end)

    :ok
  end

  test "a projection that cannot get its subscription does not damage the one that has it" do
    subscription = "contended-#{System.unique_integer([:positive])}"
    seed(200)

    projections_sup = Process.whereis(Scriba.Projections.Supervisor)
    sup_ref = Process.monitor(projections_sup)

    # The winner: takes the subscription and starts committing.
    {:ok, winner} = start(ScribaBench.Projection, subscription)
    assert wait_until(fn -> rows() > 0 end, 30_000), "winner never committed anything"
    rows_before = rows()

    # The loser: same subscription name, different projection identity. This
    # is what every node after the first sees on a rolling deploy.
    loser_start = start(StragglerProjection, subscription)

    # New work, appended after the loser has failed: the question is whether
    # the winner still picks it up. Without this the row count is static
    # simply because everything seeded was already projected.
    seed(100)

    observations = observe(winner, projections_sup, @observe_ms)

    report(subscription, loser_start, observations, rows_before)

    assert observations.projections_sup_alive,
           """
           Scriba.Projections.Supervisor died because one projection could not
           get its subscription. Every other projection in the VM went with it.
           """

    refute_received {:DOWN, ^sup_ref, :process, _, _}

    assert observations.winner_alive,
           "the winning projection died because another one lost the subscription race"

    assert observations.rows_after > rows_before,
           """
           The winner stopped committing events appended while the loser was
           failing: #{rows_before} rows before, #{observations.rows_after}
           after #{div(@observe_ms, 1000)}s.
           """
  end

  ## Observation

  defp observe(winner, projections_sup, duration_ms) do
    samples = div(duration_ms, @sample_ms)

    Enum.reduce(1..samples, initial_observations(projections_sup), fn _, acc ->
      Process.sleep(@sample_ms)

      children = DynamicSupervisor.which_children(Scriba.Projections.Supervisor)
      pids = for {_, pid, _, _} <- children, is_pid(pid), into: MapSet.new(), do: pid

      %{
        acc
        | child_pids_seen: MapSet.union(acc.child_pids_seen, pids),
          min_children: min(acc.min_children, MapSet.size(pids)),
          max_children: max(acc.max_children, MapSet.size(pids)),
          projections_sup_alive: acc.projections_sup_alive and Process.alive?(projections_sup),
          same_projections_sup:
            acc.same_projections_sup and
              Process.whereis(Scriba.Projections.Supervisor) == projections_sup,
          winner_alive: acc.winner_alive and Process.alive?(winner)
      }
    end)
    |> then(&Map.put(&1, :rows_after, rows()))
  end

  defp initial_observations(projections_sup) do
    %{
      child_pids_seen: MapSet.new(),
      min_children: 99,
      max_children: 0,
      projections_sup_alive: Process.alive?(projections_sup),
      same_projections_sup: true,
      winner_alive: true,
      rows_after: 0
    }
  end

  defp report(subscription, loser_start, o, rows_before) do
    # Distinct child pids beyond the two projections started is the restart
    # count: each restart replaces a pid.
    restarts = max(MapSet.size(o.child_pids_seen) - 2, 0)

    IO.puts("""

    === subscription contention, #{div(@observe_ms, 1000)}s observation ===
      subscription name:        #{subscription}
      loser start_projection:   #{inspect(loser_start)}
      distinct child pids seen: #{MapSet.size(o.child_pids_seen)} (≈#{restarts} restarts)
      children min/max:         #{o.min_children}/#{o.max_children}
      projections supervisor:   #{if o.projections_sup_alive, do: "alive", else: "DIED"}#{unless o.same_projections_sup, do: " (restarted — new pid)", else: ""}
      winning projection:       #{if o.winner_alive, do: "alive", else: "DIED"}
      winner rows:              #{rows_before} -> #{o.rows_after}
    """)
  end

  ## Helpers

  defp start(module, subscription) do
    Scriba.start_projection(module,
      source:
        {Scriba.Source.Commanded,
         application: CommandedApp,
         subscription_name: subscription,
         start_from: :origin,
         buffer_size: 500}
    )
  end

  defp seed(count) do
    Enum.each(1..count, fn n ->
      stream = "node-#{rem(n, 20)}-#{System.unique_integer([:positive])}"

      event = %Commanded.EventStore.EventData{
        causation_id: Commanded.UUID.uuid4(),
        correlation_id: Commanded.UUID.uuid4(),
        event_type: "Elixir.ScribaBench.Events.Ticked",
        data: %Ticked{stream: stream, n: n},
        metadata: %{}
      }

      :ok = Commanded.EventStore.append_to_stream(CommandedApp, stream, :any_version, [event])
    end)
  end

  defp rows do
    %{rows: [[n]]} = SQL.query!(Repo, "SELECT count(*) FROM bench_rows", [])
    n
  end

  defp reset_event_store do
    config = ScribaBench.EventStore.config()
    {:ok, conn} = Postgrex.start_link(config)
    EventStore.Storage.Initializer.reset!(conn, config)
    GenServer.stop(conn)
  end

  defp wait_until(fun, timeout_ms, waited \\ 0) do
    cond do
      fun.() -> true
      waited >= timeout_ms -> false
      true -> (fn -> Process.sleep(200) end).() && wait_until(fun, timeout_ms, waited + 200)
    end
  end
end
