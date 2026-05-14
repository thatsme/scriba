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

    {:ok, subscription} =
      apply(@event_store, :subscribe_to, [
        application,
        :all,
        subscription_name,
        self(),
        start_from
      ])

    state = %{
      application: application,
      subscription: subscription,
      pending: :queue.new(),
      demand: 0,
      paused: false
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(demand, state) when demand > 0 do
    dispatch(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info({:subscribed, _sub}, state), do: {:noreply, [], state}

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
  def ack(%{application: app, subscription: sub}, successful, _failed) do
    Enum.each(successful, fn msg ->
      {_module, _ref, commanded_event} = msg.acknowledger
      apply(@event_store, :ack_event, [app, sub, commanded_event])
    end)

    :ok
  end

  ## Internals

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

  defp to_message(commanded_event, state) do
    scriba_event = %Event{
      id: commanded_event.event_id,
      stream_id: commanded_event.stream_uuid,
      type: commanded_event.event_type,
      data: commanded_event.data,
      metadata: commanded_event.metadata || %{},
      position: commanded_event.event_number,
      occurred_at: commanded_event.created_at
    }

    %Broadway.Message{
      data: scriba_event,
      acknowledger:
        {__MODULE__, %{application: state.application, subscription: state.subscription},
         commanded_event}
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
