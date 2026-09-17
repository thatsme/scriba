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
    * `:buffer_size`, `:concurrency_limit`, `:partition_by` — forwarded
      verbatim to the event store adapter's subscription. Unset means the
      adapter's own default applies. See "Subscription buffer and throughput".

  ## Subscription buffer and throughput

  `:buffer_size` is how many events the store will send before it requires an
  acknowledgement. `EventStore`'s default is **1**, and that default, not
  `:parallelism`, is what bounds a projection's catch-up rate: Scriba
  acknowledges after the batch commits, so a batcher waiting on a single
  in-flight event waits out its full `:batch_timeout` before acking and
  releasing the next one.

  Measured against a real EventStore, 5,000 events over 100 streams:

  | `:buffer_size` | Throughput | 10M events |
  |---|---|---|
  | unset (adapter default, 1) | 9.1 events/sec | 12.7 days |
  | 500 | 6,002 events/sec | 28 minutes |

  Short runs read lower — 500 events reach roughly 2,200/sec, because
  startup is a larger share of the measurement.

  Scriba sets no default of its own — the adapter's applies unless configured.
  Raising it trades memory and redelivered-work-after-a-crash for throughput:
  up to `:buffer_size` events are held in flight, and an unclean restart
  replays whatever had not been acknowledged.

      source: {Scriba.Source.Commanded,
               application: MyApp.CommandedApp,
               buffer_size: 500}

  ## Acknowledgement watermark

  Two properties of this producer make a larger buffer safe.

  Acknowledgement is issued **by this process**, not by the Broadway batch
  processor that committed the batch. An event store may resolve the acking
  subscriber from `self()` and silently ignore an ack from anywhere else,
  which stalls the subscription for good once its buffer fills.

  And only the longest **gapless** run of committed events is acknowledged.
  Acks are prefix acks — acking event 7 acks everything up to 7 — while
  batches commit out of source order whenever `:parallelism` exceeds 1. A
  handler still working on event 5 must therefore hold the watermark at 4,
  however many later events have committed; otherwise a crash in that window
  loses event 5 with no dead letter, no cursor anomaly and no log line,
  because the store believes it was delivered and nothing redelivers it.

  ## Optional dependency (§11)

  This module is the only place in Scriba that depends on `:commanded`. To
  honor `optional: true`, every Commanded reference is dynamic:

    * Module atoms are built from string literals (`:"Elixir.Commanded.X"`),
      so the compiler does not record them as static module dependencies.
    * Calls go through `apply/3`.

  Result: Scriba (and this module) compile cleanly even when `:commanded` is
  absent. `start_link/1` raises a clear error in that case; `child_spec/1` is
  always safe.

  ## Watermark persistence

  The producer also records how far the projection has got. `ack_contiguous/1`
  already computes the highest gapless committed position in order to
  acknowledge safely, so persisting that number costs a write rather than a
  second calculation: it goes to `scriba_watermarks` (see `Scriba.Watermark`),
  throttled to at most one write a second while events are in flight and
  flushed immediately once the in-flight queue drains, so an idle projection
  does not sit on a stale number.

  The Pipeline supplies the repo and projection identity through a
  `:scriba_watermark` option it injects into every source's opts. A source
  that does not compute a contiguous position ignores it, and a failed write
  is logged rather than raised — the watermark is observability, and losing
  one should not cost the projection.

  ## Standby: what happens when another subscriber holds the name

  A persistent subscription admits one subscriber. Rather than failing, a
  producer that is refused stands by: it starts, retries in the background,
  and acquires the subscription when the holder releases it. On a multi-node
  deployment that is a warm standby — one node projects, the others wait,
  and a failover needs nobody's intervention.

  The curve has three phases, because two different failures share it. The
  first five attempts are milliseconds apart (50ms to 800ms), for the case
  where a producer died deliberately to force a replay and the store has not
  yet processed the DOWN. The next thirty are a second apart: a killed
  supervision tree holds its registered names until its slowest in-flight
  handler returns, so recovery can take longer than the fast attempts cover.
  Only after that does the cadence settle to about a minute with jitter, so
  standbys that started together do not retry in lockstep.

  `[:scriba, :source, :standby]` fires on every *failed* attempt and
  `[:scriba, :source, :subscribed]` when the subscription is acquired, which
  is how a takeover is observable. A standby's projection reports `:running`
  — its pipeline is up and healthy — so the telemetry, not the status, is
  what distinguishes the node doing the work from the ones waiting.

  Configuration errors are not retried. An application that is not running
  or a store that cannot be reached raises, because retrying forever would
  hide it.

  ## Pause/resume memory caveat

  `pause/1` sets a `paused: true` flag — `handle_demand/2` returns no
  messages while paused, accumulating it in the state's `:demand`. The
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
  scope for now.
  """

  @behaviour Scriba.Source

  defmodule State do
    @moduledoc false

    # The producer's state. A struct rather than a bare map so a mistyped
    # field fails at compile time: this holds the acknowledgement bookkeeping
    # that decides whether an event can be lost, and a silent nil there is
    # the most expensive kind of typo in the library.

    @enforce_keys [:application, :subscription_name, :start_from, :subscribe_opts, :producer]

    defstruct [
      :application,
      :subscription_name,
      :start_from,
      :subscribe_opts,
      :producer,
      # Where to persist the contiguous watermark, and who to persist it as.
      # nil when the source runs outside a pipeline (unit tests), or against
      # a target with no repo.
      :watermark,
      # The live subscription, once acquired. nil while standing by.
      subscription: nil,
      # Attempts made since the last successful subscribe; drives the retry
      # curve and resets on success.
      subscribe_attempt: 0,
      # Commanded sends {:subscribed, subscription} once the subscription is
      # live, explicitly so subscribers can defer work until then. Recorded
      # rather than assumed: dispatch/1 will not emit before it arrives.
      subscribed: false,
      pending: :queue.new(),
      demand: 0,
      paused: false,
      # Events dispatched downstream and not yet acknowledged to the store,
      # in delivery order, as {event_number, commanded_event}. Bounded by the
      # subscription's buffer_size.
      in_flight: :queue.new(),
      # Event numbers whose batch committed but which cannot be acknowledged
      # yet, because an earlier event has not. See ack_contiguous/1.
      committed: MapSet.new(),
      # Throttle for the watermark write: the last position written, and when.
      watermark_written: 0,
      watermark_written_at: nil
    ]
  end

  @behaviour Broadway.Acknowledger

  use GenStage

  require Logger

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

    # Forwarded verbatim to the adapter. Keyword.take rather than passing opts
    # wholesale: everything else here is Scriba's own configuration, and an
    # adapter that validates its options would reject it.
    subscribe_opts = Keyword.take(opts, [:buffer_size, :concurrency_limit, :partition_by])

    # Not subscribed here. A persistent subscription admits one subscriber, so
    # on a rolling deploy every node but one is refused — and refusing to
    # start is the wrong answer for a node whose job is to take over when the
    # holder goes away. The attempt is made after init and retried until it
    # succeeds; see handle_info(:scriba_subscribe, _).
    send(self(), :scriba_subscribe)

    state = %State{
      application: application,
      subscription_name: subscription_name,
      start_from: start_from,
      subscribe_opts: subscribe_opts,
      # This process is the event store's subscriber. ack/3 runs in a Broadway
      # batch-processor process, not here, so it needs an address to signal
      # when a batch fails to commit. Carried in every message's ack_ref.
      producer: self(),
      watermark: watermark_config(opts)
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

  # How often the contiguous watermark is written. Under load the position
  # advances with every batch; the number is for operators, who do not need
  # it to the millisecond.
  @watermark_interval_ms 1_000

  # Once the fast reap-race attempts are spent, a standby retries about once
  # a minute. The jitter keeps nodes that started together from retrying in
  # lockstep.
  @standby_interval_ms 60_000
  @standby_jitter_ms 5_000

  # After the fast attempts, one per second for half a minute. This is the
  # window in which a deliberate producer death has to recover.
  @recovery_interval_ms 1_000
  @recovery_attempts 30

  # Subscribing is retried until it succeeds, because "someone else holds
  # this subscription" is a normal state on a rolling deploy, not an error.
  # The node that loses the race stands by and takes over when the holder
  # goes away.
  #
  # The first few attempts are fast, for the reap race: a producer that died
  # deliberately to force a replay resubscribes within milliseconds, and the
  # event store may not have processed the DOWN yet. After those, the delay
  # settles at one minute with jitter — two standbys that started together
  # should not keep retrying in lockstep.
  defp attempt_subscribe(state) do
    apply(@event_store, :subscribe_to, [
      state.application,
      :all,
      state.subscription_name,
      self(),
      state.start_from,
      state.subscribe_opts
    ])
  end

  # Three phases, because two different failures share this path and they
  # want opposite things.
  #
  # A producer that died deliberately to force a replay resubscribes at once,
  # and the event store releases the old subscriber in about 100ms — so the
  # first attempts are milliseconds apart, and the next thirty are a second
  # apart. Recovery has to be quick: the projection is making no progress
  # until it resubscribes, and backing off to a minute here would stall it
  # for a minute.
  #
  # A standby on another node is a steady state that can last for weeks, so
  # after that first half-minute the cadence settles to a minute, jittered so
  # that standbys started together do not retry in lockstep.
  @doc false
  @spec subscribe_delay(pos_integer()) :: pos_integer()
  def subscribe_delay(attempt) do
    fast = length(@resubscribe_backoff)

    cond do
      attempt <= fast ->
        Enum.at(@resubscribe_backoff, attempt - 1)

      attempt <= fast + @recovery_attempts ->
        @recovery_interval_ms

      true ->
        @standby_interval_ms + :rand.uniform(@standby_jitter_ms)
    end
  end

  defp standby_message(state) do
    """
    Scriba is standing by for subscription #{inspect(state.subscription_name)}: \
    another subscriber holds it. Retrying every second for the next \
    #{@recovery_attempts}s, then about once a minute, until it is released.

    On a multi-node deployment this is expected — one node holds the
    subscription and the others take over if it goes away. If you did not
    expect it, the usual causes are:

      * Two Scriba projections sharing a :subscription_name. It defaults to
        "scriba", so give each projection against the same Commanded
        application an explicit name.

      * A commanded_ecto_projections projector still running under this name.
        Stop it, or give Scriba a different name.
    """
  end

  @impl GenStage
  def handle_demand(demand, state) when demand > 0 do
    dispatch(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info(:scriba_subscribe, state) do
    case attempt_subscribe(state) do
      {:ok, subscription} ->
        if state.subscribe_attempt > 0 do
          Logger.info(
            "Scriba acquired subscription #{inspect(state.subscription_name)} " <>
              "after #{state.subscribe_attempt} attempt(s)"
          )
        end

        :telemetry.execute(
          [:scriba, :source, :subscribed],
          %{attempts: state.subscribe_attempt},
          %{subscription: state.subscription_name}
        )

        {:noreply, [], %{state | subscription: subscription, subscribe_attempt: 0}}

      {:error, :subscription_already_exists} ->
        attempt = state.subscribe_attempt + 1
        delay = subscribe_delay(attempt)

        # Loud once, quiet after. A standby is a steady state, and a line per
        # minute per projection is noise; the telemetry event carries the
        # ongoing signal.
        if attempt == length(@resubscribe_backoff) + 1 do
          Logger.info(standby_message(state))
        end

        :telemetry.execute(
          [:scriba, :source, :standby],
          %{attempt: attempt, retry_in_ms: delay},
          %{subscription: state.subscription_name, reason: :subscription_already_exists}
        )

        Process.send_after(self(), :scriba_subscribe, delay)

        {:noreply, [], %{state | subscribe_attempt: attempt}}

      {:error, reason} ->
        # Anything else is configuration, not contention: an application that
        # is not running, a store that is not reachable. Retrying forever
        # would hide it.
        raise """
        Scriba could not subscribe to #{inspect(state.subscription_name)} on \
        #{inspect(state.application)}: #{inspect(reason)}
        """
    end
  end

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

  # Acks routed here from ack/3, which runs in a batch processor and cannot
  # acknowledge on its own behalf (see the comment there). Events arrive in
  # commit order; they are acked in that order. A failure to ack is not
  # recoverable from here and must not take the producer down — the event
  # simply stays unacknowledged and is redelivered after a restart, which is
  # the same outcome as a crash between commit and ack.
  def handle_info({:scriba_ack, events}, state) do
    committed =
      Enum.reduce(events, state.committed, fn event, set ->
        MapSet.put(set, event.event_number)
      end)

    {:noreply, [], ack_contiguous(%{state | committed: committed})}
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
    producer = Map.get(ack_ref, :producer)

    events = Enum.map(successful, fn %Broadway.Message{acknowledger: {_m, _ref, ev}} -> ev end)

    # The ack MUST be issued by the process that holds the subscription, and
    # this callback does not run there — Broadway calls it in a batch-processor
    # process. The two Commanded adapters disagree about whether that matters:
    #
    #   * InMemory takes the subscription pid as an explicit argument and
    #     ignores the caller (in_memory.ex `ack_event/3`).
    #   * EventStore resolves the subscriber from `self()`
    #     (`EventStore.Subscriptions.Subscription.ack/2` → `{:ack, n, self()}`)
    #     and its FSM drops the ack entirely when that pid is not a registered
    #     subscriber of the subscription.
    #
    # Acking from here therefore worked against InMemory — which is what the
    # test suite and the bank example run — and was silently discarded against
    # a real EventStore, stalling the subscription forever once its in-flight
    # buffer filled. Route through the producer, which is the subscriber, so
    # both adapters see an ack from a pid they recognise.
    if is_pid(producer) do
      send(producer, {:scriba_ack, events})
    else
      # No producer: a message built outside a running pipeline (unit tests).
      Enum.each(events, &apply(@event_store, :ack_event, [app, sub, &1]))
    end

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

  # Acknowledges the longest run of committed events starting at the oldest
  # unacknowledged one, and nothing past a gap.
  #
  # Acknowledging each batch as it commits is not safe, because acks are
  # prefix acks and batches do not commit in source order. With
  # `parallelism > 1` a handler can still be working on event 5 — or sleeping
  # in the retry loop — while events 6 and 7 from other streams commit. The
  # event store treats an ack for 7 as an ack for everything up to 7
  # (`EventStore.Subscriptions.Subscriber.acknowledge/2`: "All in-flight
  # events up to the ack'd event number are also ack'd"), so its checkpoint
  # moves past 5. A crash in that window loses event 5 outright: the store
  # believes it delivered it, the per-stream cursor never advanced, and
  # nothing redelivers it — no dead letter, no cursor anomaly, no log line.
  # Reproduced against a real EventStore in `bench/test/ack_loss_test.exs`.
  #
  # So the watermark only advances across a gapless prefix. Event 5 holds it
  # back until 5 itself commits, at which point 5, 6 and 7 are acknowledged
  # by a single ack for 7. A crash before that replays from below 5 and
  # pipeline-side dedup drops whatever already committed.
  defp ack_contiguous(state) do
    {last_contiguous, in_flight, committed} =
      drain_contiguous(state.in_flight, state.committed, nil)

    if last_contiguous do
      apply(@event_store, :ack_event, [state.application, state.subscription, last_contiguous])
    end

    %{state | in_flight: in_flight, committed: committed}
    |> record_watermark(last_contiguous)
  end

  # The acknowledged position IS the watermark: ack_contiguous/1 has just
  # established that everything below it is accounted for. Written outside
  # the commit transaction, so it can only lag what was applied — see
  # `Scriba.Watermark` for why that direction is the safe one.
  defp record_watermark(state, nil), do: state
  defp record_watermark(%{watermark: nil} = state, _event), do: state

  defp record_watermark(state, event) do
    position = event.event_number
    now = System.monotonic_time(:millisecond)

    # Throttled while events are still in flight, immediate once they are
    # not. Without the second condition a catch-up that finishes inside the
    # throttle window leaves the last position unwritten, and an idle
    # projection reports a stale watermark indefinitely — precisely when an
    # operator is most likely to be reading it.
    caught_up? = :queue.is_empty(state.in_flight)

    due? =
      is_nil(state.watermark_written_at) or
        now - state.watermark_written_at >= @watermark_interval_ms

    if position > state.watermark_written and (due? or caught_up?) do
      %{repo: repo, projection: projection} = state.watermark

      try do
        Scriba.Watermark.put(repo, projection, position, event.created_at)
      rescue
        # A watermark write is observability, not correctness. Losing one
        # costs a stale number until the next write; taking the producer down
        # over it would cost the projection.
        exception ->
          Logger.warning(
            "Scriba could not persist the watermark for " <>
              "#{projection.name} v#{projection.version}: #{Exception.message(exception)}"
          )
      end

      %{state | watermark_written: position, watermark_written_at: now}
    else
      state
    end
  end

  defp watermark_config(opts) do
    case Keyword.get(opts, :scriba_watermark) do
      config when is_list(config) ->
        watermark_config(Keyword.get(config, :repo), Keyword.get(config, :projection))

      _ ->
        nil
    end
  end

  # The prefix arithmetic on its own, separated from the acknowledgement it
  # feeds so it can be exercised without a store: given the in-flight queue in
  # delivery order and the set of event numbers whose batch committed, it
  # returns the last event of the gapless run, the queue past it, and the
  # committed numbers still waiting on something earlier.
  #
  # Exposed for tests rather than reached through the producer because the
  # producer's version of this ends in a call to the event store. The bug this
  # replaced acknowledged the highest committed event instead of the highest
  # contiguous one, which is a difference of one comparison and a lost event.
  @doc false
  @spec drain_contiguous(:queue.queue(), MapSet.t(), term()) ::
          {term(), :queue.queue(), MapSet.t()}
  def drain_contiguous(in_flight, committed, last) do
    case :queue.peek(in_flight) do
      {:value, {position, event}} ->
        if MapSet.member?(committed, position) do
          drain_contiguous(
            :queue.drop(in_flight),
            MapSet.delete(committed, position),
            event
          )
        else
          {last, in_flight, committed}
        end

      :empty ->
        {last, in_flight, committed}
    end
  end

  defp dispatch(%{subscribed: false} = state), do: {:noreply, [], state}
  defp dispatch(%{paused: true} = state), do: {:noreply, [], state}
  defp dispatch(%{demand: 0} = state), do: {:noreply, [], state}

  defp dispatch(state) do
    {events_to_send, remaining_pending, remaining_demand} =
      drain_queue(state.pending, state.demand, [])

    messages = Enum.map(events_to_send, &to_message(&1, state))

    # Recorded in delivery order, which is source order: the acknowledgement
    # watermark is defined against this sequence.
    in_flight =
      Enum.reduce(events_to_send, state.in_flight, fn event, q ->
        :queue.in({event.event_number, event}, q)
      end)

    {:noreply, messages,
     %{
       state
       | pending: remaining_pending,
         demand: remaining_demand,
         in_flight: in_flight
     }}
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

  # Both halves are required: a repo with no projection identity has nowhere
  # to write, and an identity with no repo has nothing to write to.
  defp watermark_config(nil, _projection), do: nil
  defp watermark_config(_repo, nil), do: nil
  defp watermark_config(repo, projection), do: %{repo: repo, projection: projection}

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
