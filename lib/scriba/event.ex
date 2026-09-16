defmodule Scriba.Event do
  @moduledoc """
  The unit of data that flows from a `Scriba.Source` through the engine to a
  `Scriba.Target`.

  A source decodes its native event into this shape and wraps it in a
  `Broadway.Message`. Handlers see the decoded `:data` and the engine routes
  events to workers by hashing `:stream_id`.

  ## `:position` is a global, monotonic, numeric ordinal

  This is the strongest assumption Scriba makes about a source, so it is
  worth stating rather than leaving implicit. `:position` must increase
  across the whole stream of events a projection consumes — not per stream —
  and comparisons on it must mean what they appear to mean. Four things rest
  on it:

    * **Dedup.** An event counts as already applied when its position is at
      or below its stream's committed cursor.
    * **Cursor monotonicity.** `GREATEST` in SQL keeps a cursor from moving
      backwards, which requires positions that order.
    * **The watermark.** "Every event up to N is accounted for" is only
      meaningful if positions can be reasoned about contiguously.
    * **Resume.** `:start_from` hands a position back to the source and
      expects everything at or below it to be filtered out.

  Commanded's global `event_number` satisfies all four, which is also what
  makes the cursor carry-over in `MIGRATION.md` work: both libraries track
  the same number.

  A store whose position is not a single increasing integer — EventStoreDB's
  commit/prepare pairs, say, or a vector clock — does not fit this shape, and
  making it fit is a design question rather than an adapter detail. Nothing
  is being built for that case speculatively; if you have such a store, open
  an issue and it can be designed against a real one. `Scriba.Target` carries
  the same reasoning for targets.
  """

  @enforce_keys [:id, :stream_id, :type, :data, :position, :occurred_at]
  defstruct [
    :id,
    :stream_id,
    :type,
    :data,
    :position,
    :occurred_at,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          stream_id: String.t(),
          type: String.t(),
          data: term(),
          metadata: map(),
          position: non_neg_integer(),
          occurred_at: DateTime.t()
        }
end
