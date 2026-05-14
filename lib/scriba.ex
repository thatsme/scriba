defmodule Scriba do
  @moduledoc """
  Public API surface for Scriba projections.

  `start_projection/1` and `list/0` come in  (the macro + dynamic
  supervisor wiring). `info/2`, `pause/2`, `resume/2`, `stop/2` are
  implemented in .
  """

  alias Scriba.Info
  alias Scriba.Projection.Coordinator

  @stream_position_truncation_limit 1000

  @doc """
  Returns a snapshot of the projection's runtime state. See `Scriba.Info` for
  the shape.

  Returns `{:error, :not_found}` if no Coordinator is registered for the
  given `(name, version)`.

  When the projection has more than #{@stream_position_truncation_limit}
  distinct streams, `:stream_positions` is `:truncated` rather than a giant
  map; `:safe_position` is always populated regardless.

  The `:status` field is one of `:initializing | :running | :paused |
  :draining | :stopped`. `:initializing` is the brief window between
  Coordinator start and Broadway producer registration; transitions to
  `:running` automatically.
  """
  @spec info(String.t(), pos_integer()) :: {:ok, Info.t()} | {:error, :not_found}
  def info(name, version \\ 1) do
    case Coordinator.get_status(name, version) do
      {:ok, status} ->
        {:ok, build_info(name, version, status)}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Pauses a running projection — the source stops yielding new events.
  In-flight events already in Pipeline processors or batchers continue
  through their commit lifecycle.

  ## Return values

    * `:ok` — pause signal sent to the source. The source's `handle_info/2`
      will run on its own schedule; by the time this function returns the
      signal is in the source's mailbox but the source may not yet have
      flipped its internal flag. Operators should NOT assume "no commits
      possible" the instant `pause/2` returns.

    * `{:error, {:invalid_state, state}}` — projection is not in a state
      where pause makes sense. The inner atom is the projection's current
      state, one of:

        - `:initializing` — engine starting up; Pipeline / producer not
          yet registered. Retry once `info/2` reports `:running`.
        - `:paused` — already paused. **No idempotency** — callers
          wanting "make sure this is paused" semantics should check
          `info/2` first or pattern-match this error case as success.
        - `:stopped` — terminal state; nothing to do.
        - `:draining` — stop in progress.

  See architecture §9.1 for the broader lifecycle.
  """
  @spec pause(String.t(), pos_integer()) ::
          :ok | {:error, {:invalid_state, atom()}}
  def pause(name, version \\ 1), do: Coordinator.pause(name, version)

  @doc """
  Resumes a paused projection — the source starts yielding new events.

  ## Return values

    * `:ok` — resume signal sent. Same asynchrony caveat as `pause/2`:
      the signal is in the source's mailbox; the first new commit follows
      whenever the source's `handle_info/2` and Broadway's processor stage
      drain accumulated demand.

    * `{:error, {:invalid_state, state}}` — projection is not paused.
      Inner atom is the current state:

        - `:initializing` — engine still starting up.
        - `:running` — already running. **No idempotency** — callers
          wanting "make sure this is running" semantics check `info/2`
          first or pattern-match this error case as success.
        - `:stopped` / `:draining` — terminal.
  """
  @spec resume(String.t(), pos_integer()) ::
          :ok | {:error, {:invalid_state, atom()}}
  def resume(name, version \\ 1), do: Coordinator.resume(name, version)

  @doc """
  Stops a running or paused projection. Waits for in-flight Broadway
  shutdown to complete before returning.

  Returns `:ok` on success, `{:error, {:invalid_state, state}}` if the
  projection is `:initializing`, `:stopped`, or `:draining`.
  """
  @spec stop(String.t(), pos_integer()) ::
          :ok | {:error, {:invalid_state, atom()}}
  def stop(name, version \\ 1), do: Coordinator.stop(name, version)

  defp build_info(name, version, status) do
    streams = Scriba.Position.stream_positions(name, version)
    safe = Scriba.Position.safe_position(name, version)

    stream_positions =
      if map_size(streams) > @stream_position_truncation_limit do
        :truncated
      else
        streams
      end

    %Info{
      name: name,
      version: version,
      status: Map.get(status, :state),
      source: Map.get(status, :source),
      target: Map.get(status, :target),
      safe_position: safe,
      stream_positions: stream_positions
    }
  end
end
