defmodule Scriba.Target.Ecto do
  @moduledoc """
  Postgres-backed read-model target.

  Builds an `Ecto.Multi` from the batch's handler results, appends one
  per-stream `scriba_positions` upsert via `Scriba.Position.multi/5` per
  stream represented in the batch, and runs everything in a single
  `Repo.transaction/1`. Read-model writes and per-stream cursor advances
  commit atomically together.

  ## Handler return contract (§4.2)

    * `:skip` — no Multi op for this event, and its stream's cursor does not
      advance (see Pipeline `stream_advances`). Whether the skip came from
      dedup or from the handler makes no difference.
    * `{:insert, schema_struct}` — `Ecto.Multi.insert/3`.
    * `{:update, schema_module, filter_keyword, [set: keyword]}` —
      `Ecto.Multi.update_all/4` filtered by `filter_keyword`.
    * `{:delete, schema_module, filter_keyword}` — `Ecto.Multi.delete_all/3`.
    * `{:multi, %Ecto.Multi{}}` — merged into the batch's Multi.

  Failure-shape results (`{:error, reason}` and the internal
  `{:exception, exception, stacktrace}` tag produced when a handler raises)
  do NOT reach `apply_handler_result/3`. Pipeline partitions them out and
  hands them to `apply_batch/6` as the `dead_letters` list; this module
  appends a `Scriba.DeadLetter.multi/4` step per dead-letter to the same
  `Ecto.Multi` that carries read-model writes and cursor advances. Result:
  one atomic transaction commits success rows, dead-letter rows, and
  advanced cursors together.

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

  # The success-shape half of §4.2 — exactly the results apply_handler_result/3
  # has a clause for. Kept adjacent to those clauses on purpose: adding a shape
  # there without adding it here silently dead-letters it, and adding it here
  # without adding it there restores the crash-loop this guard exists to
  # prevent. `{:error, _}` and the internal `{:exception, _, _}` tag are not
  # listed — the Pipeline classifies those as failures before asking.
  @impl Scriba.Target
  def valid_result?(:skip), do: true
  def valid_result?({:insert, _struct}), do: true
  def valid_result?({:update, _schema, _filter, [set: _changes]}), do: true
  def valid_result?({:delete, _schema, _filter}), do: true
  def valid_result?({:multi, %Ecto.Multi{}}), do: true
  def valid_result?(_other), do: false

  @impl Scriba.Target
  def apply_batch(
        events,
        handler_results,
        projection,
        stream_advances,
        dead_letters,
        %{repo: repo} = state
      ) do
    # Exception-free by contract. Both Multi assembly and the transaction can
    # raise rather than return: `{:insert, struct}` passes a bare struct with
    # no declared constraints, so a unique violation surfaces as a raised
    # Ecto.ConstraintError, and Ecto.Multi.merge/2 resolves lazily inside the
    # transaction. A raise here escapes to Broadway, which fails the whole
    # batch — losing the reason, and with it any chance of telling a
    # deterministic failure from a transient one. Converting to
    # `{:error, reason, state}` keeps the reason where Scriba.Failure can
    # classify it.
    multi = build_multi(events, handler_results, projection, stream_advances, dead_letters)

    case repo.transaction(multi) do
      {:ok, _changes} -> {:ok, state}
      {:error, _failed_op, reason, _changes_so_far} -> {:error, reason, state}
    end
  rescue
    exception -> {:error, exception, state}
  end

  ## Public for testability — assembles the Multi without running it

  @doc """
  Assembles the `Ecto.Multi` for a batch without running the transaction.
  Exposed so callers can inspect the structure of the assembled Multi.

  `stream_advances` is `%{stream_id => max_position}` for each stream the
  batch touches. One position-update step is appended per stream, keyed
  `{:scriba_position, stream_id}`.

  `dead_letters` is a list of `{event, error}` tuples for every per-event
  failure the Pipeline isolated: a handler that returned `{:error, _}` or
  raised, an `Ecto.Multi` operation-name collision, or a return the target
  rejected as invalid. One dead-letter step is
  appended per failed event, keyed `{:scriba_dead_letter, event.id}` (the
  same key shape `Scriba.DeadLetter.multi/4` produces).

  Order of steps in the assembled Multi:

    1. Read-model ops. `{:insert, _}`, `{:update, _, _, _}` and
       `{:delete, _, _}` each add one step keyed `{:scriba_event, event.id}`;
       `{:multi, _}` is merged under the user's own operation names, which is
       why colliding names across a batch have to be caught before assembly
       (see `{:multi_key_collision, _}` in `Scriba.DeadLetter`). `:skip` adds
       nothing.
    2. Per-stream cursor advances (one `{:scriba_position, stream_id}`).
    3. Dead-letter inserts (one `{:scriba_dead_letter, event.id}`).
  """
  @spec build_multi(
          [Scriba.Event.t()],
          [term()],
          %{name: String.t(), version: pos_integer()},
          %{String.t() => non_neg_integer()},
          [{Scriba.Event.t(), term()}]
        ) :: Ecto.Multi.t()
  def build_multi(events, handler_results, projection, stream_advances, dead_letters) do
    multi =
      events
      |> Enum.zip(handler_results)
      |> Enum.reduce(Ecto.Multi.new(), fn {event, result}, acc ->
        apply_handler_result(acc, event, result)
      end)

    multi =
      Enum.reduce(stream_advances, multi, fn {sid, pos}, acc ->
        Scriba.Position.multi(acc, projection.name, projection.version, sid, pos)
      end)

    Enum.reduce(dead_letters, multi, fn {event, error}, acc ->
      Scriba.DeadLetter.multi(acc, projection, event, error)
    end)
  end

  ## Per-event Multi step
  #
  # Only success-shape handler returns reach here. Pipeline.handle_batch
  # partitions {:error, _} and {:exception, _, _} into the dead_letters
  # argument before calling apply_batch/6, so they never appear in
  # handler_results. No clause for them — a FunctionClauseError surfaces
  # any future Pipeline partitioning bug loudly.

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

  defp build_filter_query(schema, filter) do
    Enum.reduce(filter, schema, fn {field_name, value}, query ->
      where(query, [s], field(s, ^field_name) == ^value)
    end)
  end
end
