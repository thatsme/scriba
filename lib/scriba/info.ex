defmodule Scriba.Info do
  @moduledoc """
  Snapshot of a projection's runtime state, returned by `Scriba.info/2`.

  ## Partial population in 

  In 's , only `:name`, `:version`, and the position
  fields (`:safe_position`, `:stream_positions`) are populated. The
  lifecycle and adapter fields (`:status`, `:source`, `:target`) are
  `nil` until the Coordinator's `get_status/1` call exposes them in a
  later phase.

  Once (this struct) ships, the `Scriba.info/2` caller may
  rely on the position fields. Other fields are optional context and may
  be `nil`.

  ## Truncation

  When a projection has more than 1000 distinct streams, `:stream_positions`
  becomes `:truncated` rather than a giant map. Callers needing specific
  per-stream positions in that case should use `Scriba.info/2` with a
  `streams: [list]` option (added later) to scope the lookup. `:safe_position`
  is always populated regardless of truncation.
  """

  @enforce_keys [:name, :version, :safe_position]
  defstruct [
    :name,
    :version,
    :status,
    :source,
    :target,
    :safe_position,
    :stream_positions
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          version: pos_integer(),
          status: atom() | nil,
          source: tuple() | nil,
          target: tuple() | nil,
          safe_position: non_neg_integer(),
          stream_positions: %{String.t() => non_neg_integer()} | :truncated | nil
        }
end
