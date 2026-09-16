defmodule Scriba.Target.Test do
  @moduledoc """
  In-memory `Scriba.Target` for property tests and user-facing test helpers.

  Backed by an `Agent` that holds the commit log, per-stream cursors, and
  the in-memory dead-letter list. Records every committed event as
  `{event_id, stream_id, position}` in the order it was applied. Events
  whose handler returned `:skip` are not recorded.

  Dead-letter routing: the Pipeline hands the Target a
  list of `{event, error}` tuples for events whose handler returned
  `{:error, _}` or raised. The Test target records them in its in-memory
  `:dead_letters` list, letting property
  tests assert dead-letter routing without a Repo.

  ## Usage

      {:ok, agent} = Scriba.Target.Test.start_link()

      # Wired into a projection's :target spec as
      #   {Scriba.Target.Test, agent: agent}

      Scriba.Target.Test.commits(agent)
      # => [{"evt-1", "stream-0", 1}, {"evt-2", "stream-1", 2}, ...]

      Scriba.Target.Test.dead_letters(agent)
      # => [%{event: %Scriba.Event{...}, error: {:error, :boom}}, ...]

      Scriba.Target.Test.stream_positions(agent)
      # => %{"stream-0" => 4, "stream-1" => 5}

      Scriba.Target.Test.safe_position(agent)
      # => 4   (= min across all stream cursors)
  """

  @behaviour Scriba.Target

  use Agent

  @type agent :: pid() | atom() | {:via, module(), term()}
  @type dead_letter :: %{event: Scriba.Event.t(), error: term()}

  @doc """
  Starts an in-memory commit log.

  Standard `Agent.start_link/2` options are supported (`:name`, etc).
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(
      fn -> %{commits: [], stream_positions: %{}, dead_letters: []} end,
      opts
    )
  end

  @doc """
  Returns the recorded `{event_id, stream_id, position}` tuples in the order
  they were committed.
  """
  @spec commits(agent()) :: [{String.t(), String.t(), non_neg_integer()}]
  def commits(agent) do
    agent
    |> Agent.get(& &1.commits)
    |> Enum.reverse()
  end

  @doc """
  Returns dead-letter entries in the order they were routed. Each entry is
  a `%{event: %Scriba.Event{}, error: term()}` map — the same `error`
  shape the Pipeline passed (an `{:error, reason}` tuple, or the engine-
  internal `{:exception, exception, stacktrace}` triple).
  """
  @spec dead_letters(agent()) :: [dead_letter()]
  def dead_letters(agent) do
    agent
    |> Agent.get(& &1.dead_letters)
    |> Enum.reverse()
  end

  @doc "Returns the per-stream cursor map."
  @spec stream_positions(agent()) :: %{String.t() => non_neg_integer()}
  def stream_positions(agent), do: Agent.get(agent, & &1.stream_positions)

  @doc """
  Returns the minimum cursor across the streams this target has committed to,
  or 0 when none have. Not a replay point: streams never written to contribute
  nothing, so it reads too high — the same caveat as
  `Scriba.Position.safe_position/2`.
  """
  @spec safe_position(agent()) :: non_neg_integer()
  def safe_position(agent) do
    case Agent.get(agent, & &1.stream_positions) do
      empty when map_size(empty) == 0 -> 0
      positions -> positions |> Map.values() |> Enum.min()
    end
  end

  @doc "Clears the commit log, dead-letter list, and per-stream cursors."
  @spec reset(agent()) :: :ok
  def reset(agent) do
    Agent.update(agent, fn _ ->
      %{commits: [], stream_positions: %{}, dead_letters: []}
    end)
  end

  @impl Scriba.Target
  def init(opts) do
    agent = Keyword.fetch!(opts, :agent)
    {:ok, %{agent: agent}}
  end

  @impl Scriba.Target
  def apply_batch(
        events,
        handler_results,
        _projection,
        stream_advances,
        dead_letters,
        %{agent: agent} = state
      ) do
    entries =
      events
      |> Enum.zip(handler_results)
      |> Enum.reject(fn {_event, result} -> result == :skip end)
      |> Enum.map(fn {event, _result} -> {event.id, event.stream_id, event.position} end)

    dl_entries = Enum.map(dead_letters, fn {event, error} -> %{event: event, error: error} end)

    Agent.update(agent, fn data ->
      %{
        data
        | commits: Enum.reverse(entries) ++ data.commits,
          stream_positions: Map.merge(data.stream_positions, stream_advances),
          dead_letters: Enum.reverse(dl_entries) ++ data.dead_letters
      }
    end)

    {:ok, state}
  end
end
