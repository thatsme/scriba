defmodule Scriba.Projection.Coordinator do
  @moduledoc false

  @behaviour :gen_statem

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
    # Tracks whether the `:initializing → :running` transition has fired
    # the `[:scriba, :projection, :started]` telemetry event yet. Pipeline
    # DOWN → re-initializing → re-running cycles MUST NOT re-emit
    # `:started` — operators read it as "projection came up for the first
    # time," not "Pipeline restarted." Coordinator crash resets the flag
    # to false via init/1, which is the correct semantic (the projection
    # was effectively restarted from the operator's perspective).
    started: false
  ]

  # Used only for the one-shot pipeline-pid lookup re-arm (see :state_timeout
  # handler below). Do NOT reach for :state_timeout when adding periodic
  # timers (e.g. v0.2 lag/throughput emission) — :state_timeout is reset by
  # every event in the state and silently stops firing under load. Use
  # Process.send_after(self(), :tick, interval) self-messages instead.
  # See SCRIBA_ARCHITECTURE.md §7.5 for the rationale.
  @poll_interval 50

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
      repo: Scriba.Position.resolve_repo(opts, target_spec)
    }

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

  def handle_event({:call, from}, :stop, :paused, data) do
    # After , pause keeps the Pipeline alive — :paused →
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

  def handle_event(:info, {:DOWN, _ref, _, _, _}, _state, _data),
    do: :keep_state_and_data

  ## Status query (used by Scriba.info/2)

  def handle_event({:call, from}, :get_status, state, data) do
    status = %{
      state: state,
      name: data.name,
      version: data.version,
      source: data.source_spec,
      target: data.target_spec
    }

    {:keep_state_and_data, [{:reply, from, status}]}
  end

  ## Catch-all for invalid command/state combos
  #
  # The seven cases this covers:
  #   pause from :initializing | :paused | :stopped | :draining
  #   resume from :initializing | :running | :stopped | :draining
  #   stop from :initializing | :stopped | :draining
  #
  # Uniform error shape: {:error, {:invalid_state, state}}. Inner atom
  # tells operators which state caused the rejection — different states
  # call for different remediations (retry once initialized vs already
  # paused vs terminal).

  def handle_event({:call, from}, cmd, state, _data) when cmd in [:pause, :resume, :stop] do
    {:keep_state_and_data, [{:reply, from, {:error, {:invalid_state, state}}}]}
  end

  ## Helpers

  defp transition_from_initializing(data) do
    case ensure_monitored(data) do
      {:ok, new_data} ->
        # Emit :started exactly once per Coordinator-process lifetime
        # (Pipeline DOWN → re-monitored does NOT re-fire). See defstruct
        # for the rationale.
        new_data =
          if new_data.started do
            new_data
          else
            :telemetry.execute(
              [:scriba, :projection, :started],
              %{system_time: System.system_time()},
              %{projection: %{name: new_data.name, version: new_data.version}}
            )

            %{new_data | started: true}
          end

        {:next_state, :running, new_data}

      :pending ->
        {:keep_state, data, [{:state_timeout, @poll_interval, :try_monitor}]}
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
