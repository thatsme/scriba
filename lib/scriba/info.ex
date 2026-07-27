defmodule Scriba.Info do
  @moduledoc """
  Snapshot of a projection's runtime state, returned by `Scriba.info/2`.

  ## Population

  All fields are populated when `Scriba.info/2` finds a registered
  Coordinator: `:name`, `:version` and the position fields
  (`:safe_position`, `:stream_positions`) come from the position cache;
  `:status`, `:source` and `:target` come from the Coordinator's status
  call. When no Coordinator is registered, `Scriba.info/2` returns
  `{:error, :not_found}` rather than a partially-populated struct.

  `:status` is one of `:initializing | :running | :paused | :draining |
  :stopped`. `:source` and `:target` are the `{module, opts}` specs the
  projection was started with.

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
