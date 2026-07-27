defmodule Scriba.Source.Commanded do
  @moduledoc """
  Broadway producer that subscribes to a Commanded event store and yields
  events as `%Scriba.Event{}`-wrapped `Broadway.Message`s.

  ## Usage

      source: {Scriba.Source.Commanded, application: MyApp.CommandedApp}

  ## Options

    * `:application` (required) — the user's `Commanded.Application` module.
    * `:subscription_name` (default `"scriba"`) — name passed to Commanded for
      durable subscription tracking.
    * `:start_from` (default `:origin`) — where to start reading: `:origin`,
      `:current`, or a specific event number.

  ## Optional dependency (§11)

  This module is the only place in Scriba that depends on `:commanded`. To
  honor `optional: true`, every Commanded reference is dynamic:

    * Module atoms are built from string literals (`:"Elixir.Commanded.X"`),
      so the compiler does not record them as static module dependencies.
    * Calls go through `apply/3`.

  Result: Scriba (and this module) compile cleanly even when `:commanded` is
  absent. `start_link/1` raises a clear error in that case; `child_spec/1` is
  always safe.

  ## Pause/resume memory caveat (v0.1)

  `pause/1` sets a `paused: true` flag — `handle_demand/2` returns no
  messages while paused, accumulating demand into `pending_demand`. The
  Commanded subscription, however, **keeps pushing events** into the
  source's `pending :queue` regardless of pause state (we don't
  unsubscribe).

  Memory grows during pause, bounded by how many events the upstream
  EventStore delivers in the pause window. For operator-driven pauses
  (seconds to minutes on low-volume projections), this is fine. For
  long pauses on high-throughput projections, this is a documented
  sharp edge.

  The cleaner alternative — unsubscribe on pause, re-subscribe on
  resume from the current cursor — is v0.5 hardening territory and
  changes EventStore subscription state in non-trivial ways. Out of
  scope for v0.1.
  """

  @behaviour Scriba.Source
  @behaviour Broadway.Acknowledger

  use GenStage

  alias Scriba.Event

  # Built from string literals so Elixir's compiler does not treat these as
  # static module references — keeps compilation independent of `:commanded`.
  @commanded_marker :"Elixir.Commanded.Application"
  @event_store :"Elixir.Commanded.EventStore"

  ## Source callbacks

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
    ensure_commanded_loaded!()
    GenStage.start_link(__MODULE__, opts)
  end

  ## Pause/resume

  @impl Scriba.Source
  def pause(pid) do
    send(pid, :scriba_pause)
    :ok
  end

  @impl Scriba.Source
  def resume(pid) do
    send(pid, :scriba_resume)
    :ok
  end

  ## GenStage producer callbacks

  @impl GenStage
  def init(opts) do
    application = Keyword.fetch!(opts, :application)
    subscription_name = Keyword.get(opts, :subscription_name, "scriba")
    start_from = Keyword.get(opts, :start_from, :origin)

    subscription = subscribe!(application, subscription_name, start_from)

    state = %{
      application: application,
      subscription: subscription,
      subscription_name: subscription_name,
      # This process is the event store's subscriber. ack/3 runs in a Broadway
      # batch-processor process, not here, so it needs an address to signal
      # when a batch fails to commit. Carried in every message's ack_ref.
      producer: self(),
      # Commanded sends {:subscribed, subscription} once the subscription is
      # live, explicitly so subscribers can defer work until then
      # (Commanded.EventStore.subscribe_to/5 docs). Recorded rather than
      # assumed: dispatch/1 will not emit before it arrives.
      subscribed: false,
      pending: :queue.new(),
      demand: 0,
      paused: false
    }

    {:producer, state}
  end

  # Explicit failure handling. A bare `{:ok, sub} = ...` match here produces a
  # MatchError in a :permanent producer — a crash loop whose message names
  # neither the subscription nor the cause. The already-exists case is the
  # documented migration path (old projector still running), so it gets a
  # message that says what to do.
  # Backoff for the reap race described below. Cumulative ~1.5s, which is far
  # longer than a DOWN takes to process and far shorter than a human notices.
  @resubscribe_backoff [50, 100, 200, 400, 800]

  defp subscribe!(application, subscription_name, start_from, attempts \\ @resubscribe_backoff) do
    case apply(@event_store, :subscribe_to, [
           application,
           :all,
           subscription_name,
           self(),
           start_from
         ]) do
      {:ok, subscription} ->
        subscription

      # Two very different situations produce this one error.
      #
      # The reap race: this producer just died to force a replay, and the
      # event store has not yet processed the DOWN from our previous
      # incarnation, so the subscription still looks held — by us. Transient
      # by definition, and now reachable on *every* commit failure rather
      # than only during a migration. Retry with backoff.
      #
      # A genuine conflict: another live process holds the name. Backoff will
      # not clear it, so report it after the retries are spent.
      {:error, :subscription_already_exists} when attempts != [] ->
        [delay | remaining] = attempts
        Process.sleep(delay)
        subscribe!(application, subscription_name, start_from, remaining)

      {:error, :subscription_already_exists} ->
        raise """
        Scriba could not subscribe: #{inspect(subscription_name)} is still held by \
        another process after #{length(@resubscribe_backoff)} attempts over \
        #{Enum.sum(@resubscribe_backoff)}ms.

        A persistent subscription admits one subscriber. Most likely one of:

          * Two Scriba projections share a :subscription_name. It defaults to
            "scriba", so give each projection against the same Commanded
            application an explicit name.

          * You are migrating from commanded_ecto_projections and the old
            projector is still running under this name. Stop it first, or give
            Scriba a different name:

                source: {Scriba.Source.Commanded,
                  application: #{inspect(application)},
                  subscription_name: "scriba-#{subscription_name}"}

        If instead this producer is restarting after a commit failure, the
        previous subscriber should have been reaped well within that window —
        an event store not releasing the subscription is the thing to look at.
        """

      {:error, reason} ->
        raise """
        Scriba could not subscribe to #{inspect(subscription_name)} on \
        #{inspect(application)}: #{inspect(reason)}
        """
    end
  end

  @impl GenStage
  def handle_demand(demand, state) when demand > 0 do
    dispatch(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info({:subscribed, sub}, %{subscription: sub} = state) do
    # Subscription confirmed live. Events buffered before this point (if the
    # adapter delivers early) are dispatched now rather than dropped.
    dispatch(%{state | subscribed: true})
  end

  def handle_info({:subscribed, _other}, state), do: {:noreply, [], state}

  # A batch failed to commit downstream. ack/3 (running in a Broadway batch
  # processor) refused to acknowledge it and signalled us. Raising here kills
  # the event store's subscriber process, which rewinds the subscription to
  # its last durable checkpoint; Broadway restarts this producer, it
  # resubscribes, and the batch is redelivered. See Scriba.BatchCommitError.
  def handle_info({:scriba_batch_failed, count, reason, delay}, _state) do
    # Wait before dying. Nothing downstream throttles this path — the batcher
    # re-forms a batch as soon as demand is met — so without the delay this
    # process crashes roughly ten times a second at the default
    # batch_timeout and burns the supervisor's restart budget in seconds.
    # Sleeping here is safe precisely because this process is about to die.
    if delay > 0, do: Process.sleep(delay)

    raise Scriba.BatchCommitError, count: count, reason: reason
  end

  def handle_info({:events, events}, state) do
    # Subscription keeps pushing events into pending regardless of pause
    # state. See moduledoc "Pause/resume memory caveat" for the v0.1
    # tradeoff.
    new_pending = Enum.reduce(events, state.pending, &:queue.in/2)
    dispatch(%{state | pending: new_pending})
  end

  def handle_info(:scriba_pause, state) do
    {:noreply, [], %{state | paused: true}}
  end

  def handle_info(:scriba_resume, state) do
    dispatch(%{state | paused: false})
  end

  def handle_info(_other, state), do: {:noreply, [], state}

  ## Acknowledger callback

  @impl Broadway.Acknowledger
  def ack(ack_ref, successful, [] = _failed) do
    %{application: app, subscription: sub} = ack_ref

    Enum.each(successful, fn msg ->
      {_module, _ref, commanded_event} = msg.acknowledger
      apply(@event_store, :ack_event, [app, sub, commanded_event])
    end)

    :ok
  end

  # At least one message in this batch failed — in practice the whole batch,
  # since Pipeline.handle_batch/4 fails all-or-nothing when the target's
  # transaction does not commit.
  #
  # Nothing is acknowledged, INCLUDING the successful messages. Commanded's
  # acks are prefix-acks — `ack_event/3` acknowledges "all events that precede
  # this event" (Commanded.EventStore.Adapter.ack_event/3 docs) — so there is
  # no way to acknowledge a success that sorts after a failure without
  # silently acknowledging the failure too. Acking around a gap is not
  # expressible; the only safe move is to ack nothing and replay.
  #
  # Raising here would accomplish nothing: Broadway wraps this callback in
  # try/catch and merely logs (Broadway.Topology.BatchProcessorStage). The
  # producer holds the event store subscription, so it is the producer that
  # has to die for the checkpoint to rewind. Hence the signal.
  def ack(ack_ref, _successful, failed) do
    %{subscription: sub, producer: producer} = ack_ref

    reason = failure_reason(failed)

    :telemetry.execute(
      [:scriba, :source, :batch, :failed],
      %{count: length(failed)},
      %{subscription: sub, reason: reason}
    )

    # A halt is not a replay. The Pipeline classified this failure as
    # structural — schema or permissions do not match the code — so restarting
    # to replay would loop against a condition no restart can change. Stay
    # stopped instead: nothing is acknowledged, so no event is lost, and the
    # halt has already been logged and emitted as telemetry. Recovery is a
    # deploy plus a restart, by a human.
    #
    # Producer may be absent when a message was built outside a running
    # pipeline (unit tests constructing messages via to_message/2).
    if is_pid(producer) and not halt?(reason) do
      send(producer, {:scriba_batch_failed, length(failed), reason, replay_delay(reason)})
    end

    :ok
  end

  defp halt?({:scriba_halt, _reason}), do: true
  defp halt?(reasons) when is_list(reasons), do: Enum.any?(reasons, &halt?/1)
  defp halt?(_other), do: false

  # The Pipeline computes an escalating delay for repeated transient failures
  # and ships it in the failure reason, because this process cannot remember
  # anything across its own deliberate death.
  defp replay_delay({:scriba_replay, _reason, delay}), do: delay
  defp replay_delay(reasons) when is_list(reasons) do
    reasons |> Enum.map(&replay_delay/1) |> Enum.max(fn -> 0 end)
  end

  defp replay_delay(_other), do: 0

  # Broadway.Message.failed/2 stores {:failed, reason}; a message that died by
  # raising carries {kind, reason, stacktrace}. Report the first distinct
  # reason rather than every message's copy of the same transaction error.
  defp failure_reason(failed) do
    failed
    |> Enum.map(fn
      %Broadway.Message{status: {:failed, reason}} -> reason
      %Broadway.Message{status: {_kind, reason, _stacktrace}} -> reason
      %Broadway.Message{status: status} -> status
    end)
    |> Enum.uniq()
    |> case do
      [single] -> single
      many -> many
    end
  end

  ## Internals

  defp dispatch(%{subscribed: false} = state), do: {:noreply, [], state}
  defp dispatch(%{paused: true} = state), do: {:noreply, [], state}
  defp dispatch(%{demand: 0} = state), do: {:noreply, [], state}

  defp dispatch(state) do
    {events_to_send, remaining_pending, remaining_demand} =
      drain_queue(state.pending, state.demand, [])

    messages = Enum.map(events_to_send, &to_message(&1, state))

    {:noreply, messages, %{state | pending: remaining_pending, demand: remaining_demand}}
  end

  defp drain_queue(queue, 0, acc), do: {Enum.reverse(acc), queue, 0}

  defp drain_queue(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, item}, rest} -> drain_queue(rest, demand - 1, [item | acc])
      {:empty, _} -> {Enum.reverse(acc), queue, demand}
    end
  end

  # Exposed (rather than defp) so the unit test can verify the field
  # mapping without standing up a full Broadway pipeline. The earlier
  # `stream_uuid` → `stream_id` bug went undetected because nothing in
  # the test suite exercised this function against a real RecordedEvent.
  # `state` only needs :application and :subscription — both threaded
  # into the Broadway message's acknowledger tuple. Tests pass any map
  # with those two keys.
  @doc false
  def to_message(commanded_event, state) do
    # Commanded 1.4's RecordedEvent uses :stream_id (NOT :stream_uuid —
    # that was the original bug). Field name pinned by the unit test
    # in test/scriba/source/commanded_test.exs.
    scriba_event = %Event{
      id: commanded_event.event_id,
      stream_id: commanded_event.stream_id,
      type: commanded_event.event_type,
      data: commanded_event.data,
      metadata: commanded_event.metadata || %{},
      position: commanded_event.event_number,
      occurred_at: commanded_event.created_at
    }

    %Broadway.Message{
      data: scriba_event,
      acknowledger:
        {__MODULE__,
         %{
           application: state.application,
           subscription: state.subscription,
           # nil when a message is built outside a running producer (unit
           # tests); ack/3 checks before signalling.
           producer: Map.get(state, :producer)
         }, commanded_event}
    }
  end

  defp ensure_commanded_loaded! do
    unless Code.ensure_loaded?(@commanded_marker) do
      raise """
      Scriba.Source.Commanded requires the :commanded library.

      Add it to your deps:

          {:commanded, "~> 1.4"}

      Then run `mix deps.get` and restart.
      """
    end
  end
end
