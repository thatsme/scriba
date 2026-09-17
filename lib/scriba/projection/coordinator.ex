defmodule Scriba.Projection.Coordinator do
  @moduledoc false

  @behaviour :gen_statem

  require Logger

  alias Scriba.Projection.Pipeline

  defstruct [
    :name,
    :version,
    :source_spec,
    :target_spec,
    :parallelism,
    :handler,
    :supervisor_pid,
    :pipeline_pid,
    :pipeline_ref,
    :repo,
    :lag_interval,
    # Tracks whether the `:initializing → :running` transition has fired
    # the `[:scriba, :projection, :started]` telemetry event yet. Pipeline
    # DOWN → re-initializing → re-running cycles MUST NOT re-emit
    # `:started` — operators read it as "projection came up for the first
    # time," not "Pipeline restarted." Coordinator crash resets the flag
    # to false via init/1, which is the correct semantic (the projection
    # was effectively restarted from the operator's perspective).
    started: false,
    # Set when the Pipeline reports a structural commit failure. Kept in data
    # rather than inferred, so `Scriba.info/2` can name the cause and not just
    # the state.
    halt_reason: nil,
    # Set when a Pipeline dies while the projection is paused. A pause lives
    # in the producer, and the supervisor's replacement producer starts
    # unpaused, so the instruction has to be reapplied to it — otherwise a
    # crash quietly defeats the pause. Cleared as soon as it is.
    pause_on_ready: false
  ]

  # Used only for the one-shot pipeline-pid lookup re-arm (see :state_timeout
  # handler below). Do NOT reach for :state_timeout when adding periodic
  # timers — the lag tick below is one, and :state_timeout is reset by
  # every event in the state and silently stops firing under load. Use
  # Process.send_after(self(), :tick, interval) self-messages instead.
  # See SCRIBA_ARCHITECTURE.md §7.5 for the rationale.
  @poll_interval 50

  # How often [:scriba, :projection, :lag] fires. Lag is a trend, not an
  # instant: alerting on it means "behind for a while", so a five-second
  # cadence carries the signal at a fraction of the query cost of a tighter
  # one. `lag_interval: 0` turns it off.
  @default_lag_interval 5_000

  ## Public API

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)
    :gen_statem.start_link(via_tuple(name, version), __MODULE__, opts, [])
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def via_tuple(name, version) do
    {:via, Registry, {Scriba.Registry, {:coordinator, name, version}}}
  end

  @doc """
  Reports a structural commit failure, moving the projection to `:halted`.

  Called by the Pipeline. Asynchronous on purpose: the Pipeline is inside
  `handle_batch/4` on a Broadway batch-processor process, and a synchronous
  call would let a slow or wedged Coordinator block the batcher.

  Halting is terminal. Recovery is fixing the schema or permission and
  restarting the projection, which is a deliberate human step — see
  `Scriba.Failure` for why neither replaying nor dead-lettering is safe here.
  """
  def halt(name, version, reason) do
    :gen_statem.cast(via_tuple(name, version), {:halt, reason})
  end

  def pause(name, version), do: :gen_statem.call(via_tuple(name, version), :pause)
  def resume(name, version), do: :gen_statem.call(via_tuple(name, version), :resume)
  def stop(name, version), do: :gen_statem.call(via_tuple(name, version), :stop)

  def state(name, version) do
    case Registry.lookup(Scriba.Registry, {:coordinator, name, version}) do
      [{pid, _}] ->
        {state_name, _data} = :sys.get_state(pid)
        state_name

      [] ->
        raise "coordinator not found for #{inspect({name, version})}"
    end
  end

  @doc """
  Returns `{:ok, %{state, name, version, source, target}}` for the running
  Coordinator, or `{:error, :not_found}` if no Coordinator is registered
  for `(name, version)`.

  Synchronous gen_statem call (preferred over `:sys.get_state/1` because it
  serializes through the state-machine's normal event loop and reflects
  any in-flight transitions consistently).
  """
  def get_status(name, version) do
    case Registry.lookup(Scriba.Registry, {:coordinator, name, version}) do
      [{_pid, _}] ->
        {:ok, :gen_statem.call(via_tuple(name, version), :get_status)}

      [] ->
        {:error, :not_found}
    end
  end

  ## gen_statem callbacks

  @impl :gen_statem
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl :gen_statem
  def init(opts) do
    target_spec = Keyword.fetch!(opts, :target)

    data = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      version: Keyword.fetch!(opts, :version),
      source_spec: Keyword.fetch!(opts, :source),
      target_spec: target_spec,
      parallelism: Keyword.fetch!(opts, :parallelism),
      handler: Keyword.fetch!(opts, :handler),
      supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
      repo: Scriba.Position.resolve_repo(opts, target_spec),
      lag_interval: Keyword.get(opts, :lag_interval, @default_lag_interval)
    }

    schedule_lag_tick(data)

    # Initialize the position cache once per Coordinator-process lifetime.
    # The wipe-then-preload runs here (not on every :running enter) so that
    # pause→resume preserves the cache that source-side dedup depends on.
    # Coordinator crash → init/1 re-runs → cache is wiped and reloaded from
    # Postgres, which is the crash-recovery semantic we want.
    Scriba.Position.init_cache(data.name, data.version, repo: data.repo)

    # Start in :initializing. Coordinator polls for the Pipeline's
    # registration via the @poll_interval state_timeout. Once the Pipeline
    # (specifically its Broadway producer) is registered and monitored, we
    # transition to :running. This means :running is an honest "Pipeline is
    # live and the source is reachable" — pause/resume can rely on the
    # producer pid being available without checking for nil.
    {:ok, :initializing, data, [{:next_event, :internal, :try_monitor}]}
  end

  ## :enter handlers — required by :state_enter callback mode

  @impl :gen_statem
  def handle_event(:enter, _from, :initializing, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :running, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :paused, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :draining, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :halted, _data), do: :keep_state_and_data

  def handle_event(:enter, _from, :stopped, data) do
    # Permanent stop — clean up this projection's rows in the shared cache.
    # Crash-driven Coordinator restarts are covered by init_cache/3's
    # wipe-on-entry; this drop matters for projections the user explicitly
    # stops and never restarts.
    Scriba.Position.drop_cache(data.name, data.version)
    :keep_state_and_data
  end

  ## :initializing — poll until the Pipeline (and its producer) are up

  def handle_event(:internal, :try_monitor, :initializing, data) do
    transition_from_initializing(data)
  end

  def handle_event(:state_timeout, :try_monitor, :initializing, data) do
    transition_from_initializing(data)
  end

  ## Halt — structural commit failure reported by the Pipeline
  #
  # Before this existed, a halted projection was loud exactly once (telemetry
  # plus a log line at the instant it happened) and invisible from then on:
  # `Scriba.info/2` read `:running` for a projection that would never move
  # again. Anyone not subscribed to telemetry at that moment had a stopped
  # projection and no way to see it — the same silent-stall shape the halt
  # path exists to replace, one level up.

  def handle_event(:cast, {:halt, _reason}, :halted, _data) do
    # Already halted. The Pipeline can report repeatedly — a source that
    # redelivers will re-present the same batch — and the first reason is the
    # one worth keeping.
    :keep_state_and_data
  end

  def handle_event(:cast, {:halt, reason}, _state, data) do
    {:next_state, :halted, %{data | halt_reason: reason}}
  end

  ## Lifecycle commands — valid combos
  #
  # Pause: signal source to stop yielding, emit telemetry, transition.
  # Resume: signal source to start yielding, emit telemetry, transition.
  # The source signal is asynchronous send/2 — pause/2 returns :ok as soon
  # as the signal is in the source's mailbox. In-flight events already in
  # the Pipeline processors or batchers continue through their commit
  # lifecycle. See Scriba.Source moduledoc "Pause semantics" for details.

  def handle_event({:call, from}, :pause, :running, data) do
    {source_module, _source_opts} = data.source_spec

    case Pipeline.get_producer_pid(data.name, data.version) do
      nil ->
        # Should not happen — :running means ensure_monitored succeeded —
        # but defensive: if the producer disappeared (e.g. Pipeline died
        # since our last monitor check), surface as :invalid_state.
        {:keep_state_and_data, [{:reply, from, {:error, {:invalid_state, :running}}}]}

      producer_pid ->
        :ok = source_module.pause(producer_pid)

        :telemetry.execute(
          [:scriba, :projection, :paused],
          %{system_time: System.system_time()},
          %{projection: %{name: data.name, version: data.version}}
        )

        {:next_state, :paused, data, [{:reply, from, :ok}]}
    end
  end

  def handle_event({:call, from}, :resume, :paused, data) do
    {source_module, _source_opts} = data.source_spec

    case Pipeline.get_producer_pid(data.name, data.version) do
      nil ->
        {:keep_state_and_data, [{:reply, from, {:error, {:invalid_state, :paused}}}]}

      producer_pid ->
        :ok = source_module.resume(producer_pid)

        :telemetry.execute(
          [:scriba, :projection, :resumed],
          %{system_time: System.system_time()},
          %{projection: %{name: data.name, version: data.version}}
        )

        {:next_state, :running, data, [{:reply, from, :ok}]}
    end
  end

  def handle_event({:call, from}, :stop, :running, data) do
    new_data = demonitor_pipeline(data)

    {:next_state, :draining, %{new_data | pipeline_pid: nil},
     [{:next_event, :internal, {:complete_drain, from}}]}
  end

  def handle_event({:call, from}, :stop, :halted, data) do
    # A halted projection still has a live Pipeline — halting stops
    # acknowledging, it does not tear anything down — so stopping it is the
    # same shutdown the :paused path performs. This is the operator's exit
    # from :halted once the underlying schema or permission is fixed.
    new_data = demonitor_pipeline(data)
    _ = Supervisor.terminate_child(data.supervisor_pid, Pipeline)

    {:next_state, :stopped, %{new_data | pipeline_pid: nil}, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, :stop, :paused, data) do
    # Pause keeps the Pipeline alive — :paused →
    # :stopped must terminate it. Skip the :draining state; the source
    # is already paused so no new events are entering, and the Pipeline
    # supervisor's shutdown will drain whatever's in-flight as it
    # terminates Broadway.
    new_data = demonitor_pipeline(data)
    _ = Supervisor.terminate_child(data.supervisor_pid, Pipeline)

    {:next_state, :stopped, %{new_data | pipeline_pid: nil}, [{:reply, from, :ok}]}
  end

  ## :draining: synchronous pipeline shutdown, then :stopped

  def handle_event(:internal, {:complete_drain, from}, :draining, data) do
    _ = Supervisor.terminate_child(data.supervisor_pid, Pipeline)
    {:next_state, :stopped, data, [{:reply, from, :ok}]}
  end

  ## Pipeline DOWN — observation only; rest_for_one will restart it
  #
  # On Pipeline death, transition back to :initializing so the state
  # machine honestly reflects "Pipeline is being respawned." The
  # @poll_interval state_timeout pattern in :initializing waits for the
  # new Pipeline (and its producer) to register.

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, _reason},
        :running,
        %{pipeline_ref: ref} = data
      ) do
    {:next_state, :initializing, %{data | pipeline_pid: nil, pipeline_ref: nil},
     [{:next_event, :internal, :try_monitor}]}
  end

  # A paused projection still has a live Pipeline, so it can still die — the
  # producer is built to die on commit failure to force a replay. Re-monitor
  # it the same way `:running` does, but come back to `:paused` rather than to
  # `:running`: the pause was an operator instruction, and the replacement
  # producer starts unpaused. Without this clause the projection resumes
  # processing behind the operator's back while `Scriba.info/2` still reports
  # `:paused`, and the stale ref means no later DOWN is ever matched again.
  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, _reason},
        :paused,
        %{pipeline_ref: ref} = data
      ) do
    {:next_state, :initializing,
     %{data | pipeline_pid: nil, pipeline_ref: nil, pause_on_ready: true},
     [{:next_event, :internal, :try_monitor}]}
  end

  def handle_event(:info, {:DOWN, _ref, _, _, _}, _state, _data),
    do: :keep_state_and_data

  ## Status query (used by Scriba.info/2)

  def handle_event({:call, from}, :get_status, state, data) do
    status = %{
      state: state,
      name: data.name,
      version: data.version,
      source: data.source_spec,
      target: data.target_spec,
      halt_reason: data.halt_reason
    }

    {:keep_state_and_data, [{:reply, from, status}]}
  end

  ## Catch-all for invalid command/state combos
  #
  # The thirteen cases this covers:
  #   pause from :initializing | :paused | :stopped | :draining | :halted
  #   resume from :initializing | :running | :stopped | :draining | :halted
  #   stop from :initializing | :stopped | :draining
  #
  # Uniform error shape: {:error, {:invalid_state, state}}. Inner atom
  # tells operators which state caused the rejection — different states
  # call for different remediations (retry once initialized vs already
  # paused vs terminal).

  def handle_event({:call, from}, cmd, state, _data) when cmd in [:pause, :resume, :stop] do
    {:keep_state_and_data, [{:reply, from, {:error, {:invalid_state, state}}}]}
  end

  ## Lag reporting

  # Self-message, not :state_timeout. A state_timeout is reset by every event
  # in that state, so under load — exactly when lag matters — it would be
  # pushed back indefinitely and silently stop firing. §7.5.
  def handle_event(:info, :scriba_lag_tick, state, data) do
    emit_lag(data, state)
    schedule_lag_tick(data)

    :keep_state_and_data
  end

  defp schedule_lag_tick(%{lag_interval: interval}) when interval in [0, nil], do: :ok

  defp schedule_lag_tick(%{lag_interval: interval}) do
    Process.send_after(self(), :scriba_lag_tick, interval)
    :ok
  end

  # Reading the watermark is a database round-trip on a timer, so nothing
  # here may take the Coordinator down: a repo that is briefly unreachable
  # costs a missed data point, and the next tick is five seconds away.
  #
  # No event is emitted before the projection has committed anything. A lag
  # of zero and "nothing to report yet" are different statements, and a
  # measurement that conflates them would read as caught-up.
  defp emit_lag(%{repo: nil}, _state), do: :ok

  defp emit_lag(data, state) do
    projection = %{name: data.name, version: data.version}

    case Scriba.Watermark.get(data.repo, projection) do
      %{position: position, occurred_at: %DateTime{} = occurred_at} ->
        lag_ms = DateTime.utc_now() |> DateTime.diff(occurred_at, :millisecond) |> max(0)

        :telemetry.execute(
          [:scriba, :projection, :lag],
          %{lag_ms: lag_ms, watermark: position},
          %{projection: projection, status: state}
        )

      _ ->
        :ok
    end
  rescue
    # A metric must not take the projection down: this runs on a timer, the
    # next tick is seconds away, and a repo that is briefly unreachable is not
    # the projection's problem. But swallowing it silently would hide a
    # misconfigured repo forever, so the reason is logged and the caller is
    # left alone.
    exception ->
      Logger.debug(
        "Scriba could not read the watermark for lag reporting: " <>
          Exception.message(exception)
      )

      :ok
  end

  ## Helpers

  # Exactly once per Coordinator-process lifetime: a Pipeline that goes DOWN
  # and is re-monitored does not re-fire. See defstruct for the rationale.
  defp emit_started_once(%{started: true} = data), do: data

  defp emit_started_once(data) do
    :telemetry.execute(
      [:scriba, :projection, :started],
      %{system_time: System.system_time()},
      %{projection: %{name: data.name, version: data.version}}
    )

    %{data | started: true}
  end

  defp transition_from_initializing(data) do
    case ensure_monitored(data) do
      {:ok, new_data} ->
        ready(emit_started_once(new_data))

      :pending ->
        {:keep_state, data, [{:state_timeout, @poll_interval, :try_monitor}]}
    end
  end

  # Where a newly monitored Pipeline lands: `:running`, unless a pause was in
  # force when the previous one died.
  defp ready(%{pause_on_ready: false} = data), do: {:next_state, :running, data}

  defp ready(data) do
    {source_module, _source_opts} = data.source_spec

    case Pipeline.get_producer_pid(data.name, data.version) do
      nil ->
        # Registered a moment ago in ensure_monitored/1 and gone again. Go
        # round rather than report a pause that was never applied.
        {:keep_state, data, [{:state_timeout, @poll_interval, :try_monitor}]}

      producer_pid ->
        :ok = source_module.pause(producer_pid)

        # No `[:scriba, :projection, :paused]` here: the projection is not
        # entering a pause, it is restoring one that never ended, and an
        # operator counting those events should not see a second.
        {:next_state, :paused, %{data | pause_on_ready: false}}
    end
  end

  defp ensure_monitored(%{pipeline_ref: ref} = data) when is_reference(ref) do
    {:ok, data}
  end

  defp ensure_monitored(data) do
    # We need BOTH the Pipeline supervisor pid AND the Broadway producer
    # registered. The producer is what pause/resume signals go to; without
    # it, transitioning to :running would be a lie.
    with [{pid, _}] <- Registry.lookup(Scriba.Registry, {:pipeline, data.name, data.version}),
         producer_pid when is_pid(producer_pid) <-
           Pipeline.get_producer_pid(data.name, data.version) do
      _ = producer_pid
      ref = Process.monitor(pid)
      {:ok, %{data | pipeline_pid: pid, pipeline_ref: ref}}
    else
      _ -> :pending
    end
  end

  defp demonitor_pipeline(%{pipeline_ref: nil} = data), do: data

  defp demonitor_pipeline(%{pipeline_ref: ref} = data) do
    Process.demonitor(ref, [:flush])
    %{data | pipeline_ref: nil}
  end
end
