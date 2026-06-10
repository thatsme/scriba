defmodule Scriba.DeadLetter do
  @moduledoc """
  Helpers for the `scriba_dead_letters` table — failed events recorded for
  later inspection or replay.

  An event arrives here in three ways (§9):

    1. The handler returned `{:error, reason}`.
    2. The handler raised — the engine catches and converts to `{:error, exception}`.
    3. The target's atomic commit (e.g. `Repo.transaction/1`) failed.

  All three end up routed to a row in `scriba_dead_letters` with a serialized
  copy of the event and an error description. Per §9.2, the projection's
  position **advances past the dead-lettered event** — the projection does not
  block. Replaying dead-letters is a v0.2 concern.

  This module exposes raw helpers; routing logic (when to insert) lives in the
  Pipeline once the Ecto target is wired in.
  """

  alias Ecto.Adapters.SQL
  alias Scriba.Event

  @type projection :: %{name: String.t(), version: pos_integer()}
  @type error :: %{
          kind: String.t(),
          message: String.t() | nil,
          stacktrace: String.t() | nil
        }

  @doc """
  Builds a row map for the `scriba_dead_letters` table from a failing event
  and an error tuple/exception.

  Accepts:
    * `{:error, reason}` — `kind = "error"`, `message = inspect(reason)`
    * `{:exception, exception, stacktrace}` — `kind = exception.__struct__`,
      message + formatted stacktrace
  """
  @spec build_row(projection(), Event.t(), term()) :: map()
  def build_row(%{name: name, version: version}, %Event{} = event, error) do
    err = normalize_error(error)

    %{
      projection_name: name,
      projection_version: version,
      position: event.position,
      stream_id: event.stream_id,
      event_type: event.type,
      event_data: serialize_event_data(event.data),
      error_kind: err.kind,
      error_message: err.message,
      error_stacktrace: err.stacktrace,
      occurred_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
    }
  end

  @doc """
  Inserts a single dead-letter row directly via `repo`. Used by callers that
  manage their own transaction (e.g. retry-then-dead-letter flows).
  """
  @spec insert(module(), projection(), Event.t(), term()) :: {:ok, term()} | {:error, term()}
  def insert(repo, projection, %Event{} = event, error) do
    row = build_row(projection, event, error)

    SQL.query(
      repo,
      """
      INSERT INTO scriba_dead_letters
        (projection_name, projection_version, position, stream_id,
         event_type, event_data, error_kind, error_message, error_stacktrace,
         occurred_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      """,
      [
        row.projection_name,
        row.projection_version,
        row.position,
        row.stream_id,
        row.event_type,
        row.event_data,
        row.error_kind,
        row.error_message,
        row.error_stacktrace,
        row.occurred_at
      ]
    )
  end

  @doc """
  Appends a dead-letter insert to an `Ecto.Multi`. Useful for the Pipeline's
  "dead-letter and advance position" transaction after a target apply_batch
  has already failed and rolled back.
  """
  @spec multi(Ecto.Multi.t(), projection(), Event.t(), term()) :: Ecto.Multi.t()
  def multi(multi, projection, %Event{} = event, error) do
    Ecto.Multi.run(multi, {:scriba_dead_letter, event.id}, fn repo, _changes ->
      insert(repo, projection, event, error)
    end)
  end

  ## Internals

  defp normalize_error({:error, reason}) do
    %{kind: "error", message: inspect(reason), stacktrace: nil}
  end

  defp normalize_error({:exception, exception, stacktrace}) when is_exception(exception) do
    %{
      kind: exception.__struct__ |> Atom.to_string(),
      message: Exception.message(exception),
      stacktrace: Exception.format_stacktrace(stacktrace)
    }
  end

  defp normalize_error(other) do
    %{kind: "unknown", message: inspect(other), stacktrace: nil}
  end

  defp serialize_event_data(data) when is_map(data) and not is_struct(data), do: data

  defp serialize_event_data(data) when is_struct(data) do
    data
    |> Map.from_struct()
    |> Map.put(:__struct__, data.__struct__ |> Atom.to_string())
  end

  defp serialize_event_data(data), do: %{value: inspect(data)}
end
