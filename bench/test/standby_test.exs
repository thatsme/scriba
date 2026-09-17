defmodule ScribaBench.StandbyTest do
  @moduledoc """
  Does a node that loses the subscription race take over when the holder
  goes away?

  This is the rolling-deploy shape. A persistent subscription admits one
  subscriber, so one node holds it and the others have nothing to do — until
  the holder dies, at which point one of them has to pick it up without
  anybody intervening.

  An earlier experiment (`multi_node_contention_test.exs`) established what
  used to happen: the loser's producer raised during init, the supervisor
  never adopted it, and `start_projection/1` returned an error to the caller.
  No crash loop — but no standby either. Nothing retried, so a failover had
  nobody to fail over to.

  What is asserted here is the other half: the loser starts, stands by, and
  takes over. One BEAM rather than three, so this is the subscription
  handover rather than a true multi-node failover — but the subscription is
  held in the event store and shared by every node, so the contention and
  the handover are the real ones.
  """

  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias ScribaBench.{CommandedApp, Repo}
  alias ScribaBench.Events.Ticked
  alias ScribaBench.Wait

  setup do
    reset_event_store()
    SQL.query!(Repo, "TRUNCATE bench_rows, scriba_positions, scriba_dead_letters", [])
    SQL.query!(Repo, "DELETE FROM scriba_watermarks", [])

    ref = make_ref()

    :telemetry.attach_many(
      {:standby_test, ref},
      [[:scriba, :source, :standby], [:scriba, :source, :subscribed]],
      &__MODULE__.forward/4,
      %{test_pid: self(), ref: ref}
    )

    on_exit(fn ->
      :telemetry.detach({:standby_test, ref})

      for {_, pid, _, _} <- DynamicSupervisor.which_children(Scriba.Projections.Supervisor) do
        DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
      end
    end)

    %{ref: ref}
  end

  @doc false
  def forward(event, measurements, metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, List.last(event), measurements, metadata})
  end

  test "the loser stands by, then takes over when the holder goes away", %{ref: ref} do
    subscription = "standby-#{System.unique_integer([:positive])}"
    seed(50)

    {:ok, holder} = start(ScribaBench.Projection, subscription)
    assert_receive {^ref, :subscribed, _, %{subscription: ^subscription}}, 10_000
    assert Wait.until(fn -> rows() > 0 end, 30_000), "the holder never committed"

    # The standby starts rather than failing, which is the change: it is a
    # supervised, running projection that simply has no subscription yet.
    assert {:ok, standby} = start(ScribaBench.StragglerProjection, subscription)
    assert Process.alive?(standby)

    assert_receive {^ref, :standby, %{attempt: attempt}, %{subscription: ^subscription}}, 10_000
    assert attempt >= 1

    # Take the holder away, as a deploy or a crash would.
    ref_holder = Process.monitor(holder)
    DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, holder)
    assert_receive {:DOWN, ^ref_holder, :process, _, _}, 10_000

    # Nobody intervenes: the standby's own retry picks it up.
    assert_receive {^ref, :subscribed, %{attempts: attempts}, %{subscription: ^subscription}},
                   30_000

    assert attempts >= 1, "the takeover should have come from a retry, not a first attempt"

    rows_before = rows()
    seed(25)

    assert Wait.until(fn -> rows() > rows_before end, 30_000),
           """
           The standby acquired the subscription but never committed anything:
           #{rows()} rows, was #{rows_before}. A standby that takes over and
           then does nothing is worse than one that never started.
           """
  end

  test "a standby retries on a bounded schedule rather than spinning", %{ref: ref} do
    subscription = "standby-#{System.unique_integer([:positive])}"

    {:ok, _holder} = start(ScribaBench.Projection, subscription)
    assert_receive {^ref, :subscribed, _, _}, 10_000

    {:ok, _standby} = start(ScribaBench.StragglerProjection, subscription)

    # Collect the standby attempts over a couple of seconds. The fast
    # reap-race attempts come first; after those the delay has to settle,
    # or a node that stands by for a day hammers the event store.
    # Six, not five: the first five are the fast reap-race attempts, and the
    # sixth is where the standby cadence starts.
    delays =
      for _ <- 1..6 do
        assert_receive {^ref, :standby, %{retry_in_ms: delay}, _}, 10_000
        delay
      end

    assert delays == [50, 100, 200, 400, 800, 1000],
           """
           The retry schedule changed: #{inspect(delays)}

           Expected the five fast reap-race attempts, then the one-second
           recovery cadence. The minute-long standby cadence starts too late
           to assert here; `Scriba.Source.Commanded.subscribe_delay/1` is
           unit-tested for the whole curve.
           """
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
      stream = "sb-#{rem(n, 10)}-#{System.unique_integer([:positive])}"

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
end
