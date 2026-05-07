defmodule Scriba.Test.Source do
  @moduledoc """
  In-memory `Scriba.Source` for fast tests and property-test harnesses.

  Behaves like a tiny Commanded subscription:

    * Holds a finite list of `%Scriba.Event{}` values.
    * On `start_link/1`, accepts an optional `:start_from` cursor; only
      events whose `position` is strictly greater than `start_from` are
      ever yielded by this source's instance.
    * Implements `Broadway.Acknowledger`; a successful ack advances the
      source's `acked_cursor` to the maximum acked position. Failed acks
      do not advance the cursor.
    * The cursor lives in this process's GenStage state. It dies with the
      source — mirroring Commanded's behaviour where tearing down a
      subscription forgets its cursor. To resume from a previous run,
      pass the previously-observed `acked_cursor/1` value as `:start_from`
      on the next `start_link/1`.

  ## Why a cursor

  (source-side dedup) in  added a Pipeline-
  level dedup against the projection's per-stream cursors. To exercise it
  meaningfully, the source must be able to *not* replay events that the
  projection has already committed — same shape as Commanded resuming a
  named subscription from its acked position. The fixed `:start_from`
  + ack-driven cursor advance gives test harnesses control of that
  resume point.
  """

  @behaviour Scriba.Source
  @behaviour Broadway.Acknowledger

  use GenStage

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
    GenStage.start_link(__MODULE__, opts)
  end

  ## Public inspection API

  @doc """
  Returns the source's current `acked_cursor` — the maximum position
  whose ack has been received. Suitable for use as `:start_from` on a
  subsequent `start_link/1`.
  """
  @spec acked_cursor(GenServer.server()) :: non_neg_integer()
  def acked_cursor(source), do: GenStage.call(source, :acked_cursor)

  ## GenStage producer callbacks

  @impl GenStage
  def init(opts) do
    events = Keyword.get(opts, :events, [])
    start_from = Keyword.get(opts, :start_from, 0)

    # Filter events at init: anything at or below start_from is
    # considered "already acked by a prior incarnation" and never yielded.
    queue = Enum.filter(events, fn e -> e.position > start_from end)

    state = %{
      queue: queue,
      acked_cursor: start_from
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(demand, %{queue: queue} = state) when demand > 0 do
    {to_send, remaining} = Enum.split(queue, demand)
    messages = Enum.map(to_send, &to_message(&1, self()))

    {:noreply, messages, %{state | queue: remaining}}
  end

  @impl GenStage
  def handle_call(:acked_cursor, _from, state) do
    {:reply, state.acked_cursor, [], state}
  end

  @impl GenStage
  def handle_info({:advance_acked_cursor, position}, state) do
    new_acked = max(state.acked_cursor, position)
    {:noreply, [], %{state | acked_cursor: new_acked}}
  end

  def handle_info(_other, state), do: {:noreply, [], state}

  ## Acknowledger callback

  @impl Broadway.Acknowledger
  def ack(source_pid, successful, _failed) do
    # Per-message data carries the event position; lift the max from the
    # successful set and ask the source to advance. Async send rather than
    # synchronous call so ack/3 (called from Broadway processor processes)
    # never blocks on the source.
    case successful do
      [] ->
        :ok

      msgs ->
        max_pos =
          msgs
          |> Enum.map(fn msg ->
            {_mod, _ref, position} = msg.acknowledger
            position
          end)
          |> Enum.max()

        send(source_pid, {:advance_acked_cursor, max_pos})
        :ok
    end
  end

  ## Internals

  defp to_message(%Scriba.Event{} = event, source_pid) do
    %Broadway.Message{
      data: event,
      # `acknowledger: {module, ack_ref, ack_data}`. ack_ref is the source
      # pid so ack/3 can route the cursor-advance message back. ack_data
      # is the event position so the source knows how far to advance.
      acknowledger: {__MODULE__, source_pid, event.position}
    }
  end
end
