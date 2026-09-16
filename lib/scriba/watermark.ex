defmodule Scriba.Watermark do
  @moduledoc """
  The contiguous global position a projection has reached.

  Per-stream cursors (`scriba_positions`) answer "where is this stream?".
  They cannot answer "where is this projection?", because a minimum across
  them counts only streams the projection has written to, and a maximum
  counts work that may sit above an event still in flight. The watermark is
  the highest position `P` such that **every** event up to `P` has been
  accounted for — committed, skipped, or dead-lettered — with no gap below it.

  That single number is what makes three things possible: how far behind a
  projection is, how far a rebuild has got, and where a replica could resume
  without missing anything.

  ## It lags reality, and only in the safe direction

  The source computes the watermark when it acknowledges (see
  `Scriba.Source.Commanded`), and persists it outside the commit
  transaction. A crash between a commit and the write leaves the stored
  watermark *behind* what was actually applied, never ahead. Resuming from a
  stale watermark redelivers events that already committed, which dedup
  absorbs; resuming from one that ran ahead would skip events that never did.
  Only one of those is recoverable, so the write is deliberately not atomic
  with the commit.

  Writes are also throttled — a projection committing thousands of events a
  second does not need thousands of watermark rows a second — so the stored
  value trails the in-memory one by up to `Scriba.Source.Commanded`'s write
  interval even while everything is healthy.

  ## occurred_at

  Alongside the position, the row carries the `occurred_at` of the event at
  that position, when the source knows it. `now() - occurred_at` is the
  projection's lag in time, which is what operators alert on, and it needs no
  knowledge of where the event store's head is — something Commanded's
  adapter behaviour does not expose.
  """

  import Ecto.Query, only: [from: 2]

  @type projection :: %{name: String.t(), version: pos_integer()}

  @doc """
  Records `position` as the projection's watermark, if it is ahead of what is
  already stored.

  `GREATEST` in SQL rather than a read-then-write: two producers for the same
  projection should never both be running, but if one lingers through a
  handover its late write must not move the watermark backwards.
  """
  @spec put(module(), projection(), non_neg_integer(), DateTime.t() | nil) :: :ok
  def put(repo, %{name: name, version: version}, position, occurred_at \\ nil) do
    now = DateTime.utc_now()

    repo.query!(
      """
      INSERT INTO scriba_watermarks
        (projection_name, projection_version, position, occurred_at, updated_at)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (projection_name, projection_version)
      DO UPDATE SET position = GREATEST(scriba_watermarks.position, EXCLUDED.position),
                    occurred_at = CASE
                      WHEN EXCLUDED.position >= scriba_watermarks.position
                      THEN EXCLUDED.occurred_at
                      ELSE scriba_watermarks.occurred_at
                    END,
                    updated_at = EXCLUDED.updated_at
      """,
      [name, version, position, occurred_at, now]
    )

    :ok
  end

  @doc """
  Reads the stored watermark, or `nil` when the projection has never written
  one — a projection that has not yet committed anything, or one whose source
  does not report a watermark.
  """
  @spec get(module(), projection()) ::
          %{position: non_neg_integer(), occurred_at: DateTime.t() | nil} | nil
  def get(repo, %{name: name, version: version}) do
    query =
      from(w in "scriba_watermarks",
        where: w.projection_name == ^name and w.projection_version == ^version,
        select: %{position: w.position, occurred_at: w.occurred_at}
      )

    case repo.one(query) do
      nil ->
        nil

      %{position: position, occurred_at: occurred_at} ->
        %{position: position, occurred_at: as_utc(occurred_at)}
    end
  end

  # `:utc_datetime_usec` is `timestamp` without a time zone in Postgres, and a
  # schemaless query has no field type to cast by, so Postgrex returns a
  # NaiveDateTime. Callers are promised a DateTime — including lag_ms/2, which
  # would raise on the naive one.
  defp as_utc(nil), do: nil
  defp as_utc(%DateTime{} = datetime), do: datetime
  defp as_utc(%NaiveDateTime{} = naive), do: DateTime.from_naive!(naive, "Etc/UTC")

  @doc """
  Lag in milliseconds: how long ago the event at the watermark happened.

  `nil` when there is no watermark yet, or when the source did not report an
  `occurred_at`. This measures time, not events — an idle projection that is
  fully caught up reports the age of the last event it saw, which is the
  number an operator wants when asking "is anything still flowing?".
  """
  @spec lag_ms(module(), projection()) :: non_neg_integer() | nil
  def lag_ms(repo, projection) do
    case get(repo, projection) do
      %{occurred_at: %DateTime{} = occurred_at} ->
        DateTime.utc_now()
        |> DateTime.diff(occurred_at, :millisecond)
        |> max(0)

      _ ->
        nil
    end
  end
end
