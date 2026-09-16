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
  :halted | :stopped`. `:source` and `:target` are the `{module, opts}` specs
  the projection was started with.

  ## `:halted` is the one to alert on

  A halted projection hit a commit failure that neither replaying nor
  dead-lettering can resolve — a missing column, a missing privilege. It has
  stopped acknowledging events and will not move again until the cause is
  fixed and the projection restarted. Nothing is lost: no event is
  acknowledged and no cursor advances.

  `:halt_reason` carries the underlying error (typically a `Postgrex.Error`
  whose SQLSTATE names the cause) and is `nil` in every other state. Polling
  `status` is enough to detect it — you do not have to have been subscribed to
  `[:scriba, :projection, :halted]` at the instant it fired.

  ## `:safe_position` is introspection, not a replay point

  It is the minimum across the streams currently in the position cache — a
  rough "how far behind is the laggard" figure. It reads **too high** in two
  ways: the cache preloads at most 10,000 streams, and streams this
  projection has never written to contribute nothing at all. Do not resume a
  replica from it. A real replay point is v0.3 work and needs an uncapped
  `MIN(position)` aggregate against Postgres.

  ## Truncation

  When a projection has more than 1000 distinct streams, `:stream_positions`
  becomes `:truncated` rather than a giant map. There is no option to scope
  the lookup to specific streams; callers needing per-stream positions past
  that threshold read `scriba_positions` directly. `:safe_position` is always
  populated regardless of truncation.
  """

  @enforce_keys [:name, :version, :safe_position]
  defstruct [
    :name,
    :version,
    :status,
    :source,
    :target,
    :safe_position,
    :stream_positions,
    :halt_reason
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          version: pos_integer(),
          status: atom() | nil,
          source: tuple() | nil,
          target: tuple() | nil,
          safe_position: non_neg_integer(),
          stream_positions: %{String.t() => non_neg_integer()} | :truncated | nil,
          halt_reason: term() | nil
        }
end
