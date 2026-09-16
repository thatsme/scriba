defmodule Scriba.DeadLetter do
  @moduledoc """
  Helpers for the `scriba_dead_letters` table — failed events recorded for
  later inspection or replay.

  An event arrives here in five ways (§9). Two are handler failures, after
  retry exhaustion:

    1. The handler returned `{:error, reason}`.
    2. The handler raised — the engine catches it and tags it
       `{:exception, exception, stacktrace}`.

  Three more bypass the retry layer entirely, because retrying them changes
  nothing:

    3. The commit failed with an integrity-class error, isolated by the
       per-event fallback pass (`{:commit_error, reason}`).
    4. The handler returned a `{:multi, _}` whose operation names collide
       with another event's in the same batch (`{:multi_key_collision, keys}`).
    5. The handler returned a shape the target cannot apply.

  All of them end up routed to a row in `scriba_dead_letters` with a serialized
  copy of the event and an error description. Per §9.2, the projection's
  position **advances past the dead-lettered event** — the projection does not
  block. There is no replay function yet; replaying means reading the row and
  re-dispatching the event yourself.

  Transient and structural commit failures are *not* dead-letter paths: a
  transient one replays the whole batch, a structural one halts the
  projection. See `Scriba.Target`.

  This module exposes raw helpers; the routing decision (when to insert) lives
  in the Pipeline, which partitions failure-shape handler results out of the
  batch and passes them to the target as `dead_letters`.
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
    * `{:exception, exception, stacktrace}` — `kind` is the exception module
      as a string, message + formatted stacktrace
    * `{:commit_error, reason}` — `kind = "commit:<SQLSTATE label>"`
    * `{:multi_key_collision, keys}` — `kind = "multi_key_collision"`
    * anything else — `kind = "invalid_return"`
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

  # An integrity violation (constraint, invalid data) that the per-event
  # fallback isolated to this event. The batch failed, the fallback re-applied
  # it alone, and it failed alone — so the event's data is bad against the
  # current schema and no amount of replaying will change that.
  defp normalize_error({:commit_error, reason}) do
    %{
      kind: "commit:" <> Scriba.Failure.label(reason),
      message: Exception.format_banner(:error, reason),
      stacktrace: nil
    }
  end

  defp normalize_error({:multi_key_collision, keys}) do
    %{
      kind: "multi_key_collision",
      message: """
      Ecto.Multi operation name(s) #{inspect(keys)} were already claimed by an \
      earlier event in the same batch, or are reserved by Scriba.

      Scriba merges every event's {:multi, _} into one batch transaction, so \
      operation names must be unique across the batch — not just within one \
      event's Multi. commanded_ecto_projections ran one transaction per event, \
      where a static name was safe; that is the usual source of this.

      Key the operation by something event-unique, e.g. {:my_op, meta.id}.\
      """,
      stacktrace: nil
    }
  end

  # A handler return outside §4.2's six shapes. The Pipeline's target-backed
  # validity check routes these here rather than letting them raise inside the
  # Target, where the failure would be a batch-level crash-loop instead of a
  # per-event dead letter. The inspected value is the diagnostic — it is what
  # the handler actually returned.
  defp normalize_error(other) do
    %{kind: "invalid_return", message: inspect(other), stacktrace: nil}
  end

  defp serialize_event_data(data) when is_map(data) and not is_struct(data), do: data

  defp serialize_event_data(data) when is_struct(data) do
    data
    |> Map.from_struct()
    |> Map.put(:__struct__, data.__struct__ |> Atom.to_string())
  end

  defp serialize_event_data(data), do: %{value: inspect(data)}
end
