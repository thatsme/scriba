defmodule Scriba.Test.PropertyHelpers do
  @moduledoc false

  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Target.Test, as: TestTarget

  @doc """
  Default per-projection opts for property tests. Small batches + short
  timeout keep iterations quick.
  """
  def projection_opts(events, agent, name, overrides \\ []) do
    [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {TestTarget, agent: agent},
      parallelism: 4,
      handler: Scriba.Test.Projection,
      batch_size: 5,
      batch_timeout: 30
    ]
    |> Keyword.merge(overrides)
  end

  @doc """
  Starts a per-projection Supervisor and unlinks it from the test process so
  a `:kill` later doesn't propagate.

  Polls `Scriba.Registry` until any prior {coordinator, pipeline, projection_supervisor}
  registrations under this name+version have been cleaned up. Also retries on
  child-start failures (the inner Broadway pipeline can race with Registry
  even after our outer registrations look clean).

  trap_exits during start_link so a failing init doesn't kill the caller.
  """
  def start_projection(opts, retries \\ 20) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)

    wait_for_clean_registry(name, version)

    prior_trap = Process.flag(:trap_exit, true)

    result =
      case ProjSup.start_link(opts) do
        {:ok, sup} ->
          Process.unlink(sup)
          # Drain any stray EXIT message the link may have queued.
          flush_exits()
          {:ok, sup}

        {:error, _} when retries > 0 ->
          Process.flag(:trap_exit, prior_trap)
          flush_exits()
          Process.sleep(25)
          start_projection(opts, retries - 1)

        {:error, _} = err ->
          err
      end

    Process.flag(:trap_exit, prior_trap)
    result
  end

  defp wait_for_clean_registry(name, version, timeout_ms \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_clean(name, version, deadline)
  end

  defp do_wait_clean(name, version, deadline) do
    keys = [
      {:pipeline, name, version},
      {:coordinator, name, version},
      {:projection_supervisor, name, version}
    ]

    cond do
      Enum.all?(keys, &(Registry.lookup(Scriba.Registry, &1) == [])) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        :timeout

      true ->
        Process.sleep(5)
        do_wait_clean(name, version, deadline)
    end
  end

  defp flush_exits do
    receive do
      {:EXIT, _pid, _reason} -> flush_exits()
    after
      0 -> :ok
    end
  end

  @doc """
  Hard-kills the supervisor (simulates a crash). Used by P3 only — Broadway
  emits error logs when its tree is `:kill`ed, so wrap callers in
  `@moduletag capture_log: true`.
  """
  def kill_and_wait(sup, timeout \\ 1_000) do
    await_exit(sup, :kill, timeout)
    Process.sleep(10)
    :ok
  end

  @doc """
  Graceful supervisor shutdown for end-of-iteration cleanup. Falls back to
  `:kill` if the tree doesn't terminate within `timeout`.
  """
  def stop_projection(sup, timeout \\ 1_000) do
    case await_exit(sup, :shutdown, timeout) do
      :ok ->
        Process.sleep(10)
        :ok

      :timeout ->
        await_exit(sup, :kill, timeout)
        Process.sleep(10)
        :killed_after_timeout
    end
  end

  defp await_exit(sup, reason, timeout) do
    ref = Process.monitor(sup)
    Process.exit(sup, reason)

    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        :timeout
    end
  end

  @doc """
  Polls the Test target's commit log until at least `expected` distinct
  event_ids have been recorded, or `timeout_ms` elapses.
  """
  def wait_until_unique(agent, expected, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait(agent, expected, deadline)
  end

  defp do_wait(agent, expected, deadline) do
    unique = unique_count(agent)

    cond do
      unique >= expected ->
        {:ok, unique}

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout, unique, expected}

      true ->
        Process.sleep(20)
        do_wait(agent, expected, deadline)
    end
  end

  defp unique_count(agent) do
    agent
    |> TestTarget.commits()
    |> Enum.map(&elem(&1, 0))
    |> MapSet.new()
    |> MapSet.size()
  end

  @doc "Sorted list of distinct event_ids in the Test target's commit log."
  def unique_event_ids(commits) do
    commits |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort()
  end
end
