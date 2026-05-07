defmodule Scriba.Target.Test do
  @moduledoc """
  In-memory `Scriba.Target` for property tests and user-facing test helpers.

  Backed by an `Agent` that holds the commit log and per-stream cursors.
  Records every committed event as `{event_id, stream_id, position}` in the
  order it was applied. Events whose handler returned `:skip` are not
  recorded but still advance the stream's cursor.

  ## Usage

      {:ok, agent} = Scriba.Target.Test.start_link()

      # Wired into a projection's :target spec as
      #   {Scriba.Target.Test, agent: agent}

      Scriba.Target.Test.commits(agent)
      # => [{"evt-1", "stream-0", 1}, {"evt-2", "stream-1", 2}, ...]

      Scriba.Target.Test.stream_positions(agent)
      # => %{"stream-0" => 4, "stream-1" => 5}

      Scriba.Target.Test.safe_position(agent)
      # => 4   (= min across all stream cursors)
  """

  @behaviour Scriba.Target

  use Agent

  @type agent :: pid() | atom() | {:via, module(), term()}

  @doc """
  Starts an in-memory commit log.

  Standard `Agent.start_link/2` options are supported (`:name`, etc).
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> %{commits: [], stream_positions: %{}} end, opts)
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

  @doc "Returns the per-stream cursor map."
  @spec stream_positions(agent()) :: %{String.t() => non_neg_integer()}
  def stream_positions(agent), do: Agent.get(agent, & &1.stream_positions)

  @doc """
  Returns the safe replay point: the minimum cursor across all streams. 0
  when no streams have committed yet.
  """
  @spec safe_position(agent()) :: non_neg_integer()
  def safe_position(agent) do
    case Agent.get(agent, & &1.stream_positions) do
      empty when map_size(empty) == 0 -> 0
      positions -> positions |> Map.values() |> Enum.min()
    end
  end

  @doc "Clears the commit log and resets per-stream cursors."
  @spec reset(agent()) :: :ok
  def reset(agent) do
    Agent.update(agent, fn _ -> %{commits: [], stream_positions: %{}} end)
  end

  @impl Scriba.Target
  def init(opts) do
    agent = Keyword.fetch!(opts, :agent)
    {:ok, %{agent: agent}}
  end

  @impl Scriba.Target
  def apply_batch(events, handler_results, _projection, stream_advances, %{agent: agent} = state) do
    entries =
      events
      |> Enum.zip(handler_results)
      |> Enum.reject(fn {_event, result} -> result == :skip end)
      |> Enum.map(fn {event, _result} -> {event.id, event.stream_id, event.position} end)

    Agent.update(agent, fn data ->
      %{
        data
        | commits: Enum.reverse(entries) ++ data.commits,
          stream_positions: Map.merge(data.stream_positions, stream_advances)
      }
    end)

    {:ok, state}
  end
end
