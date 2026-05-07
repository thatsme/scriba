defmodule Scriba.Event do
  @moduledoc """
  The unit of data that flows from a `Scriba.Source` through the engine to a
  `Scriba.Target`.

  A source decodes its native event into this shape and wraps it in a
  `Broadway.Message`. Handlers see the decoded `:data` and the engine routes
  events to workers by hashing `:stream_id`.
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
