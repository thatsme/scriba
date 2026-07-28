defmodule Scriba.Projection.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)
    Supervisor.start_link(__MODULE__, opts, name: via_tuple(name, version))
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  def via_tuple(name, version) do
    {:via, Registry, {Scriba.Registry, {:projection_supervisor, name, version}}}
  end

  # Restart intensity, chosen rather than defaulted — and it only means what
  # it says because Scriba.Circuit throttles the crash rate. The two have to
  # be read together.
  #
  # A commit failure kills the source's producer on purpose, to rewind the
  # subscription and replay. Unthrottled that repeats at whatever rate batches
  # form — ~10 times a second at `batch_timeout: 100` — and then *no* restart
  # budget helps: 30 restarts would be three seconds, 300 would be thirty. The
  # size of the number was never the lever.
  #
  # The lever is the delay Scriba.Circuit escalates before each death:
  # 0, 100ms, 500ms, 1s, 5s, 15s, then 30s per attempt. Across 30 restarts
  # that is roughly twelve minutes, so this budget genuinely spans a
  # multi-minute outage instead of appearing to.
  #
  # Exhausting it is still the right end state: this supervisor's death
  # propagates to Scriba.Projections.Supervisor and toward the host
  # application, which is correct for a projection that has spent twelve
  # minutes unable to commit. By then the halt path, the dead-letter table and
  # [:scriba, :source, :batch, :failed] have all had ample chance to say so.
  #
  # OTP's 3-in-5 default would instead take the host application down over a
  # one-second database blip.
  @max_restarts 30
  @max_seconds 60

  @impl Supervisor
  def init(opts) do
    coordinator_opts = Keyword.put(opts, :supervisor_pid, self())

    children = [
      {Scriba.Projection.Coordinator, coordinator_opts},
      {Scriba.Projection.Pipeline, opts}
    ]

    Supervisor.init(children,
      strategy: :rest_for_one,
      max_restarts: @max_restarts,
      max_seconds: @max_seconds
    )
  end
end
