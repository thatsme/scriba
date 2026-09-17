defmodule Scriba.Circuit do
  @moduledoc """
  Per-projection failure state that has to outlive the producer.

  The source's producer dies on purpose to rewind its subscription, so it
  cannot remember anything across a commit failure. Two decisions need memory
  it does not have:

    * **How long to wait before dying again.** Without a delay the transient
      path crashes as fast as batches form — roughly ten times a second at
      `batch_timeout: 100` — which burns a supervisor's restart budget in
      seconds regardless of how generous that budget is.

    * **Whether repeated single-event integrity failures are one bad row or a
      systemic mismatch.** See `record_wipeout/3`.

  State lives in one shared ETS table for the whole BEAM, keyed by
  `{name, version}`, following `Scriba.Position`'s model: created once at
  application start, one atom regardless of projection count.
  """

  @table __MODULE__.Table

  # Escalating delay before the producer dies. Cumulative across 30 restarts
  # this is ~12 minutes, which is what makes Scriba.Projection.Supervisor's
  # 30-per-60s budget mean what its comment claims.
  @backoff [0, 100, 500, 1_000, 5_000, 15_000]
  @max_backoff 30_000

  # A single-event batch that fails on integrity is indistinguishable from one
  # genuinely bad row, so it dead-letters. If it keeps happening with nothing
  # ever committing, it is not one bad row.
  @wipeout_threshold 3

  @doc false
  @spec create_table() :: :ok
  def create_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:set, :public, :named_table, {:write_concurrency, true}])
        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Records a transient commit failure and returns how long the producer should
  wait before dying, in milliseconds.
  """
  @spec record_transient(String.t(), pos_integer()) :: non_neg_integer()
  def record_transient(name, version) do
    n = bump(name, version, :transient)
    Enum.at(@backoff, n - 1, @max_backoff)
  end

  @doc """
  Records a batch in which **every** attempted write failed on integrity
  grounds and none committed, and returns whether the projection should halt.

  SQLSTATE says a failure is deterministic and event-specific. It does not say
  how many events share the defect. A tightened column type or a `NOT NULL`
  added to a field the handler never populates makes *every* insert fail with
  a class 22/23 code — each one individually dead-letterable, so the per-event
  fallback would drain the entire stream into `scriba_dead_letters`, advance
  the cursor to head, and leave `Scriba.info/2` reporting a fully caught-up
  projection over an empty read model. That is precisely the outcome
  `:structural` exists to prevent, reached through a different code class.

  Blast radius is the guard that SQLSTATE cannot provide:

    * More than one event attempted and all of them failed → systemic, halt
      now. One poison row does not take its whole batch with it.
    * Exactly one event attempted → ambiguous, so dead-letter it. If it keeps
      happening with nothing ever committing (#{@wipeout_threshold} batches
      running), it is systemic after all.
  """
  @spec record_wipeout(String.t(), pos_integer(), pos_integer()) :: :halt | :continue
  def record_wipeout(name, version, attempted) when attempted > 1 do
    _ = bump(name, version, :wipeout)
    :halt
  end

  def record_wipeout(name, version, _attempted) do
    if bump(name, version, :wipeout) >= @wipeout_threshold, do: :halt, else: :continue
  end

  @doc "Clears all failure state — called whenever a batch commits."
  @spec reset(String.t(), pos_integer()) :: :ok
  def reset(name, version) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, {name, version})
    end

    :ok
  end

  defp bump(name, version, counter) do
    if :ets.whereis(@table) == :undefined do
      1
    else
      key = {name, version}
      current = :ets.lookup_element(@table, key, 2, %{})
      n = Map.get(current, counter, 0) + 1
      :ets.insert(@table, {key, Map.put(current, counter, n)})
      n
    end
  end
end
