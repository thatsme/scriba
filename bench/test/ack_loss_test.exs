defmodule ScribaBench.AckLossTest do
  @moduledoc """
  Does a straggling event survive a crash that happens after later events
  have been acknowledged?

  Commanded acknowledgements are prefix acknowledgements: acking event 7
  acknowledges everything at or below 7. Scriba acknowledges per committed
  batch, and with `parallelism > 1` batches commit out of source order — a
  handler still working on event 1 does not stop events 2..N from
  committing and acking. The subscription checkpoint then sits above an
  event that never committed. Nothing redelivers it after a restart: the
  store believes it is done, the per-stream cursor never advanced, and
  source-side dedup has nothing to dedup.

  This test induces exactly that: one event on a slow stream, many later
  events on fast streams, a hard kill while the slow one is still inside its
  handler, then a restart under the same subscription.

  The invariant asserted is the one the library exists to provide — every
  delivered event ends up either in the read model or in the dead-letter
  table, never neither.

  ## Why this cannot live in the main suite

  It needs a real event store. `Commanded.EventStore.Adapters.InMemory`
  holds one event in flight per subscriber, so no later event can overtake
  a straggler and there is no window to test.

  Before 0.1.2 this window was nearly unreachable for a second reason:
  `EventStore`'s own `buffer_size` default is 1, which serialises delivery
  the same way. 0.1.2 forwards `:buffer_size`, and the documentation
  recommends raising it for throughput — which is what makes this window
  reachable in practice.
  """

  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias ScribaBench.{CommandedApp, Repo, StragglerProjection}
  alias ScribaBench.Events.Ticked
  alias ScribaBench.Wait

  @fast_events 300
  @fast_streams 30

  setup do
    # The event store must start empty. It is durable and shared across runs,
    # and a subscription from :origin replays everything in it — including
    # every previous run's straggler, each of which sleeps for the handler
    # delay. That turns "was the event lost?" into "did the replay finish
    # inside the timeout?", which is a different question and a false
    # negative waiting to happen.
    reset_event_store()
    SQL.query!(Repo, "TRUNCATE bench_rows, scriba_positions, scriba_dead_letters", [])
    on_exit(fn -> stop_projection() end)
    :ok
  end

  defp reset_event_store do
    config = ScribaBench.EventStore.config()
    {:ok, conn} = Postgrex.start_link(config)
    EventStore.Storage.Initializer.reset!(conn, config)
    GenServer.stop(conn)
  end

  test "an event still in its handler survives a crash that acked later events" do
    subscription = "ackloss-#{System.unique_integer([:positive])}"
    slow_stream = "slow-#{System.unique_integer([:positive])}"

    # The straggler is appended FIRST, so it holds the lowest position of the
    # run. Everything appended after it sorts above it, and acking any of
    # them acknowledges past it.
    append(slow_stream, 1)
    fast_total = append_fast()

    start_projection(subscription)

    # Wait for a batch of fast events to commit. Every one of them sorts
    # above the straggler, so acknowledging that batch acknowledges past an
    # event that has not committed. Waiting for ALL of them would not work:
    # per-stream ordering hashes streams onto processors, so the fast streams
    # sharing the straggler's processor are queued behind it by design.
    # Ten, not fifty: the guard only has to establish that batches were
    # committing while the straggler was still inside its handler. Waiting
    # for a large share of them makes the setup race the straggler's own
    # timer under load, and the test then fails for having taken too long
    # rather than for losing an event.
    assert Wait.until(fn -> count_fast() >= 10 end, 30_000),
           "no fast events committed: #{count_fast()}/#{fast_total}"

    assert count_slow(slow_stream) == 0,
           "the straggler committed too early — raise @straggler_delay_ms"

    # Hard kill, mid-handler: the node dies between the ack and the
    # straggler's commit.
    kill_projection()

    # Restart under the same subscription name. The event store resumes from
    # its own checkpoint, which is what makes or breaks the event.
    start_projection(subscription)

    recovered? =
      Wait.until(
        fn ->
          count_slow(slow_stream) == 1 or dead_lettered?(slow_stream)
        end,
        StragglerProjection.straggler_delay_ms() + 30_000
      )

    assert recovered?, """
    Event lost.

    The straggler was never redelivered after the restart: it is absent from
    the read model and from scriba_dead_letters, and no cursor records it.
    Acknowledging a later event checkpointed the subscription past an event
    that had not committed.

      stream:            #{slow_stream}
      read-model rows:   #{count_slow(slow_stream)}
      dead letters:      #{count_dead_letters(slow_stream)}
      fast rows present: #{count_fast()}/#{fast_total}

    Fix: acknowledge only the highest contiguous (gapless) committed
    position, rather than each committed batch as it lands.
    """
  end

  ## Helpers

  defp append(stream, n) do
    event = %Commanded.EventStore.EventData{
      causation_id: Commanded.UUID.uuid4(),
      correlation_id: Commanded.UUID.uuid4(),
      event_type: "Elixir.ScribaBench.Events.Ticked",
      data: %Ticked{stream: stream, n: n},
      metadata: %{}
    }

    :ok = Commanded.EventStore.append_to_stream(CommandedApp, stream, :any_version, [event])
  end

  defp append_fast do
    per_stream = div(@fast_events, @fast_streams)

    Enum.each(1..@fast_streams, fn s ->
      stream = "fast-#{s}-#{System.unique_integer([:positive])}"
      Enum.each(1..per_stream, fn n -> append(stream, n) end)
    end)

    per_stream * @fast_streams
  end

  # The killed tree's Broadway processes deregister asynchronously, so a
  # restart immediately after a hard kill can lose a race with its own
  # corpse and fail with :already_started. That is a property of killing a
  # supervision tree from outside, not of the behaviour under test.
  defp start_projection(subscription, attempts_left \\ 40) do
    case do_start(subscription) do
      {:ok, pid} ->
        {:ok, pid}

      # Covers {:error, reason} and {:error, :already_started, pid} alike.
      _retryable when attempts_left > 0 ->
        Process.sleep(250)
        start_projection(subscription, attempts_left - 1)

      other ->
        flunk("could not start projection: #{inspect(other)}")
    end
  end

  defp do_start(subscription) do
      Scriba.start_projection(StragglerProjection,
        source:
          {Scriba.Source.Commanded,
           application: CommandedApp,
           subscription_name: subscription,
           start_from: :origin,
           # High enough that the whole run is in flight at once. This is the
           # configuration the README recommends for throughput.
           buffer_size: 1_000}
      )
  end

  defp projection_pid do
    Scriba.Projections.Supervisor
    |> DynamicSupervisor.which_children()
    |> case do
      [{_, pid, _, _} | _] when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp kill_projection do
    pid = projection_pid()
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> flunk("projection did not die")
    end

    # Wait for the corpse to deregister rather than sleeping a guessed
    # interval. Killing a supervision tree tears its children down
    # asynchronously, and Broadway refuses to start a topology whose via-tuple
    # names are still held — so a restart races the tree it is replacing.
    # Wait for the old tree to deregister every one of its Broadway processes,
    # not just the producer. A :kill on the supervisor reaches the supervisors
    # as a trappable exit, so they shut their children down in an orderly way
    # — and a processor sleeping inside the straggler's handler does not
    # return for eight seconds. Until those names are free, Broadway refuses
    # to start a replacement topology, so a restart that does not wait here
    # spends the window colliding with the tree it is replacing.
    Wait.until(
      fn ->
        Scriba.Internals.Registry
        |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
        |> Enum.all?(fn
          {"ackloss", 1, _} -> false
          _ -> true
        end)
      end,
      30_000
    )
  end

  defp stop_projection do
    case projection_pid() do
      nil -> :ok
      pid -> DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
    end
  end

  defp count_fast do
    %{rows: [[n]]} =
      SQL.query!(Repo, "SELECT count(*) FROM bench_rows WHERE stream LIKE 'fast-%'", [])

    n
  end

  defp count_slow(stream) do
    %{rows: [[n]]} =
      SQL.query!(Repo, "SELECT count(*) FROM bench_rows WHERE stream = $1", [stream])

    n
  end

  defp count_dead_letters(stream) do
    %{rows: [[n]]} =
      SQL.query!(Repo, "SELECT count(*) FROM scriba_dead_letters WHERE stream_id = $1", [stream])

    n
  end

  defp dead_lettered?(stream), do: count_dead_letters(stream) > 0
end
