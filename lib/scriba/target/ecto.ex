defmodule Scriba.Target.Ecto do
  @moduledoc """
  Postgres-backed read-model target.

  Builds an `Ecto.Multi` from the batch's handler results, appends one
  per-stream `scriba_positions` upsert via `Scriba.Position.multi/5` per
  stream represented in the batch, and runs everything in a single
  `Repo.transaction/1`. Read-model writes and per-stream cursor advances
  commit atomically together.

  ## Handler return contract (§4.2)

    * `:skip` — no Multi op for this event (its stream's cursor still advances).
    * `{:insert, schema_struct}` — `Ecto.Multi.insert/3`.
    * `{:update, schema_module, filter_keyword, [set: keyword]}` —
      `Ecto.Multi.update_all/4` filtered by `filter_keyword`.
    * `{:delete, schema_module, filter_keyword}` — `Ecto.Multi.delete_all/3`.
    * `{:multi, %Ecto.Multi{}}` — merged into the batch's Multi.
    * `{:error, reason}` — appended as a step that fails the transaction.

  ## Usage

      target: {Scriba.Target.Ecto, repo: MyApp.Repo}
  """

  @behaviour Scriba.Target

  import Ecto.Query, only: [where: 3]

  ## Target callbacks

  @impl Scriba.Target
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    {:ok, %{repo: repo}}
  end

  @impl Scriba.Target
  def apply_batch(events, handler_results, projection, stream_advances, %{repo: repo} = state) do
    multi = build_multi(events, handler_results, projection, stream_advances)

    case repo.transaction(multi) do
      {:ok, _changes} ->
        {:ok, state}

      {:error, _failed_op, reason, _changes_so_far} ->
        {:error, reason, state}
    end
  end

  ## Public for testability — assembles the Multi without running it

  @doc """
  Assembles the `Ecto.Multi` for a batch without running the transaction.
  Exposed so callers can inspect the structure of the assembled Multi.

  `stream_advances` is `%{stream_id => max_position}` for each stream the
  batch touches. One position-update step is appended per stream, keyed
  `{:scriba_position, stream_id}`.
  """
  @spec build_multi(
          [Scriba.Event.t()],
          [term()],
          %{name: String.t(), version: pos_integer()},
          %{String.t() => non_neg_integer()}
        ) :: Ecto.Multi.t()
  def build_multi(events, handler_results, projection, stream_advances) do
    multi =
      events
      |> Enum.zip(handler_results)
      |> Enum.reduce(Ecto.Multi.new(), fn {event, result}, acc ->
        apply_handler_result(acc, event, result)
      end)

    Enum.reduce(stream_advances, multi, fn {sid, pos}, acc ->
      Scriba.Position.multi(acc, projection.name, projection.version, sid, pos)
    end)
  end

  ## Per-event Multi step

  defp apply_handler_result(multi, _event, :skip), do: multi

  defp apply_handler_result(multi, event, {:insert, struct}) do
    Ecto.Multi.insert(multi, {:scriba_event, event.id}, struct)
  end

  defp apply_handler_result(multi, event, {:update, schema, filter, [set: changes]}) do
    query = build_filter_query(schema, filter)
    Ecto.Multi.update_all(multi, {:scriba_event, event.id}, query, set: changes)
  end

  defp apply_handler_result(multi, event, {:delete, schema, filter}) do
    query = build_filter_query(schema, filter)
    Ecto.Multi.delete_all(multi, {:scriba_event, event.id}, query)
  end

  defp apply_handler_result(multi, _event, {:multi, %Ecto.Multi{} = user_multi}) do
    Ecto.Multi.merge(multi, fn _changes -> user_multi end)
  end

  defp apply_handler_result(multi, event, {:error, reason}) do
    Ecto.Multi.run(multi, {:scriba_event, event.id}, fn _repo, _changes ->
      {:error, reason}
    end)
  end

  defp build_filter_query(schema, filter) do
    Enum.reduce(filter, schema, fn {field_name, value}, query ->
      where(query, [s], field(s, ^field_name) == ^value)
    end)
  end
end
