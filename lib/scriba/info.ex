defmodule Scriba.Info do
  @moduledoc """
  Snapshot of a projection's runtime state, returned by `Scriba.info/2`.

  ## Population

  `Scriba.info/2` populates every field it can when it finds a registered
  Coordinator: `:name`, `:version` and the position fields
  (`:safe_position`, `:stream_positions`) come from the position cache;
  `:status`, `:source` and `:target` come from the Coordinator's status
  call; `:watermark` and `:lag_ms` are read from `scriba_watermarks`.

  Three fields are legitimately `nil`: `:halt_reason` outside `:halted`, and
  `:watermark`/`:lag_ms` before the projection has committed anything or for
  a source that reports no watermark.

  When no Coordinator is registered, `Scriba.info/2` returns
  `{:error, :not_found}` rather than a partially-populated struct.

  `:status` is one of `:initializing | :running | :paused | :draining |
  :halted | :stopped`. `:source` and `:target` are the `{module, opts}` specs
  the projection was started with.

  ## `:halted` is the one to alert on

  A halted projection hit a commit failure that neither replaying nor
  dead-lettering can resolve — a missing column, a missing privilege, or a
  batch in which every attempted write failed on integrity grounds. It has
  stopped acknowledging events and will not move again until the cause is
  fixed and the projection restarted. Nothing is lost: no event is
  acknowledged and no cursor advances.

  `:halt_reason` carries the underlying error — usually a `Postgrex.Error`
  whose SQLSTATE names the cause, or `{:integrity_wipeout, n}` when a whole
  batch failed on integrity grounds — and is `nil` in every other state. Polling
  `status` is enough to detect it — you do not have to have been subscribed to
  `[:scriba, :projection, :halted]` at the instant it fired.

  ## `:safe_position` is introspection, not a replay point

  It is the minimum across the streams currently in the position cache — a
  rough "how far behind is the laggard" figure. It reads **too high** in two
  ways: the cache preloads at most 10,000 streams, and streams this
  projection has never written to contribute nothing at all. Do not resume a
  replica from it. A real replay point is v0.3 work and needs an uncapped
  `MIN(position)` aggregate against Postgres.

  ## `:watermark` and `:lag_ms` — how far along, and how far behind

  `:watermark` is the contiguous global position: every event up to it has
  been committed, skipped or dead-lettered, with no gap below. Unlike
  `:safe_position` it is a number a replica could resume from, and unlike
  `:stream_positions` it answers for the projection rather than per stream.
  `:lag_ms` is how long ago the event at that position happened.

  Both are `nil` until the projection has committed something, and both stay
  `nil` for a source that does not report a watermark — `Scriba.Source.Commanded`
  does, `Scriba.Test.Source` does not. They are read from `scriba_watermarks`,
  written outside the commit transaction and throttled, so they trail reality
  slightly even when everything is healthy. See `Scriba.Watermark`.

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
    :halt_reason,
    :watermark,
    :lag_ms
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          version: pos_integer(),
          status: atom() | nil,
          source: tuple() | nil,
          target: tuple() | nil,
          safe_position: non_neg_integer(),
          stream_positions: %{String.t() => non_neg_integer()} | :truncated | nil,
          halt_reason: term() | nil,
          watermark: non_neg_integer() | nil,
          lag_ms: non_neg_integer() | nil
        }
end
