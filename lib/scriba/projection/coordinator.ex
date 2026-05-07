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
    :repo
  ]

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
      repo: resolve_repo(opts, target_spec)
    }

    {:ok, :idle, data, [{:next_event, :internal, :auto_start}]}
  end

  # Resolve the :repo the Coordinator hands to Position.init_cache.
  #
  # Order:
  #   1. Explicit `:repo` opt on the projection wins (override path; e.g. a
  #      custom target that also wants cache preload from Postgres).
  #   2. Scriba.Target.Ecto's target_spec carries `:repo` in its target opts —
  #      extract it so Ecto-target projections always have a repo for cache
  #      rebuild on Coordinator restart. This is what bounds the ETS-lag-
  #      behind-Postgres window after crashes.
  #   3. Otherwise nil (e.g. Test target — Agent is its own truth, no cache
  #      rebuild needed).
  defp resolve_repo(opts, target_spec) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} -> repo
      :error -> repo_from_target_spec(target_spec)
    end
  end

  defp repo_from_target_spec({Scriba.Target.Ecto, target_opts}) when is_list(target_opts) do
    # Scriba.Target.Ecto.init/1 itself raises if :repo is missing, but the
    # Coordinator runs first and needs the repo for cache preload — so we
    # surface a clear error here rather than letting init/1 fail later.
    Keyword.fetch!(target_opts, :repo)
  end

  defp repo_from_target_spec(_), do: nil

  ## :enter handlers — required by :state_enter callback mode

  @impl :gen_statem
  def handle_event(:enter, _from, :idle, _data), do: :keep_state_and_data

  def handle_event(:enter, _from, :running, data) do
    Scriba.Position.init_cache(data.name, data.version, repo: data.repo)

    case ensure_monitored(data) do
      {:ok, new_data} -> {:keep_state, new_data}
      :pending -> {:keep_state, data, [{:state_timeout, @poll_interval, :monitor_pipeline}]}
    end
  end

  def handle_event(:enter, _from, :paused, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :draining, _data), do: :keep_state_and_data
  def handle_event(:enter, _from, :stopped, _data), do: :keep_state_and_data

  ## :idle → :running

  def handle_event(:internal, :auto_start, :idle, data) do
    {:next_state, :running, data}
  end

  ## Pipeline-pid lookup polling while in :running

  def handle_event(:state_timeout, :monitor_pipeline, :running, data) do
    case ensure_monitored(data) do
      {:ok, new_data} -> {:keep_state, new_data}
      :pending -> {:keep_state, data, [{:state_timeout, @poll_interval, :monitor_pipeline}]}
    end
  end

  ## Lifecycle commands — valid combos

  def handle_event({:call, from}, :pause, :running, data) do
    new_data = demonitor_pipeline(data)
    _ = Supervisor.terminate_child(data.supervisor_pid, Pipeline)

    {:next_state, :paused, %{new_data | pipeline_pid: nil}, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, :resume, :paused, data) do
    case Supervisor.restart_child(data.supervisor_pid, Pipeline) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        new_data = %{data | pipeline_pid: pid, pipeline_ref: ref}
        {:next_state, :running, new_data, [{:reply, from, :ok}]}

      {:error, reason} ->
        {:keep_state_and_data, [{:reply, from, {:error, reason}}]}
    end
  end

  def handle_event({:call, from}, :stop, :running, data) do
    new_data = demonitor_pipeline(data)

    {:next_state, :draining, %{new_data | pipeline_pid: nil},
     [{:next_event, :internal, {:complete_drain, from}}]}
  end

  def handle_event({:call, from}, :stop, :paused, data) do
    {:next_state, :stopped, data, [{:reply, from, :ok}]}
  end

  ## :draining: synchronous pipeline shutdown, then :stopped

  def handle_event(:internal, {:complete_drain, from}, :draining, data) do
    _ = Supervisor.terminate_child(data.supervisor_pid, Pipeline)
    {:next_state, :stopped, data, [{:reply, from, :ok}]}
  end

  ## Pipeline DOWN — observation only; rest_for_one will restart it

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, _reason},
        :running,
        %{pipeline_ref: ref} = data
      ) do
    {:keep_state, %{data | pipeline_pid: nil, pipeline_ref: nil},
     [{:state_timeout, @poll_interval, :monitor_pipeline}]}
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

  def handle_event({:call, from}, cmd, state, _data) when cmd in [:pause, :resume, :stop] do
    {:keep_state_and_data, [{:reply, from, {:error, {:invalid_state, state}}}]}
  end

  ## Helpers

  defp ensure_monitored(%{pipeline_ref: ref} = data) when is_reference(ref) do
    {:ok, data}
  end

  defp ensure_monitored(data) do
    case Registry.lookup(Scriba.Registry, {:pipeline, data.name, data.version}) do
      [{pid, _}] ->
        ref = Process.monitor(pid)
        {:ok, %{data | pipeline_pid: pid, pipeline_ref: ref}}

      [] ->
        :pending
    end
  end

  defp demonitor_pipeline(%{pipeline_ref: nil} = data), do: data

  defp demonitor_pipeline(%{pipeline_ref: ref} = data) do
    Process.demonitor(ref, [:flush])
    %{data | pipeline_ref: nil}
  end
end
