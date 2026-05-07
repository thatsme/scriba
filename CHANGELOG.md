# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-10

Initial release. Projection engine for Elixir event-sourced systems
that pairs with Commanded, replacing the role
[`commanded_ecto_projections`](https://github.com/commanded/commanded-ecto-projections)
filled before it stopped being actively maintained.

### Added

#### Public API

- `use Scriba.Projection` macro. Five-line declaration generates the
  full projection module:
  ```elixir
  use Scriba.Projection,
    name: "orders",
    source: {Scriba.Source.Commanded, application: MyApp.CommandedApp},
    target: {Scriba.Target.Ecto, repo: MyApp.Repo},
    parallelism: 16
  ```
  Compile-time validation rejects unknown options, validates `:partition_by`
  is `:stream_id` (the only value supported in v0.1), and warns when
  `:name` matches the legacy `_v<integer>$` suffix pattern from
  `commanded_ecto_projections`.
- `Scriba.start_projection/1` and `/2`. Adds the projection to
  `Scriba.Projections.Supervisor` (a `DynamicSupervisor`) at runtime;
  `/2` accepts overrides for non-identity options (`:parallelism`,
  `:retry`, etc.) and rejects `:name` / `:version` overrides since
  identity is compile-time.
- `Scriba.pause/1`, `resume/1`, `stop/1`, `info/1`, `list/0`. Each
  accepts either a projection module (reads identity from the
  generated `__scriba_config__/0`) or a string name (defaults to
  version 1). `pause/2`, `resume/2`, `stop/2`, `info/2` take explicit
  `(name, version)` for operator workflows.

#### Engine internals

- Per-stream cursor tracking in `scriba_positions`. One row per
  `(projection_name, projection_version, stream_id)` — cross-partition
  commit ordering can no longer cause silent drops because each stream
  has its own cursor.
- Atomic position + read-model commit. Every successful batch commits
  the user's read-model writes AND every touched stream's cursor
  advance in **one** `Ecto.Multi` transaction. No observable state
  where the read model advanced but the cursor didn't, or vice versa.
- Consistent-hash partitioning by `stream_id` via
  `Scriba.Partitioner.partition/2`. Events on the same stream
  deterministically route to the same Broadway processor; events
  across streams parallelize up to `:parallelism`.
- Source-side dedup at `Pipeline.handle_message/3`. Events whose
  position is at or below the committed cursor for their stream
  return `:skip` without invoking the handler — protects against
  source redelivery after Pipeline restart.
- Shared ETS cache for hot-path cursor reads
  (`Scriba.Position.Cache`). One named table for the whole BEAM,
  keyed by `{name, version, stream_id}` tuples. Replaces the earlier
  per-projection named-table design that allocated one BEAM atom per
  projection.

#### Source adapters

- `Scriba.Source.Commanded`. Subscribes to a Commanded.Application
  via `Commanded.EventStore.subscribe_to/5`; converts
  `Commanded.EventStore.RecordedEvent` structs into Broadway messages
  with `%Scriba.Event{}` data. Acknowledges back to Commanded after
  the Multi commits. `:commanded` is an `optional: true` dep — Scriba
  compiles and runs without it.
- `Scriba.Source.Commanded.pause/1` and `resume/1` callbacks. Pause
  flips an internal flag; `handle_demand/2` accumulates demand
  without dispatching until resume. The EventStore subscription
  keeps pushing into the source's queue during pause — bounded
  memory growth documented in the module's moduledoc.

#### Target adapters

- `Scriba.Target.Ecto`. Builds an `Ecto.Multi` from per-event handler
  results plus per-stream cursor advances plus per-event dead-letter
  inserts; runs everything in a single `Repo.transaction/1`. Handler
  returns become Multi steps:
    - `:skip` → no step (cursor still advances per-stream).
    - `{:insert, struct}` → `Ecto.Multi.insert/3`.
    - `{:update, schema, filter, [set: changes]}` → `Ecto.Multi.update_all/4`.
    - `{:delete, schema, filter}` → `Ecto.Multi.delete_all/3`.
    - `{:multi, %Ecto.Multi{}}` → merged via `Ecto.Multi.merge/2`.
- `Scriba.Target.Test`. In-memory `Agent`-backed target for property
  tests and downstream user testing. Records commits, per-stream
  positions, and dead-letter entries.

#### Operational features

- **Telemetry events** (full surface in `Scriba.Telemetry`'s moduledoc):
    - `[:scriba, :projection, :event, :start | :stop | :exception]` —
      via `:telemetry.span/3` around each handler invocation. Fires
      per retry attempt.
    - `[:scriba, :projection, :batch, :stop]` — manual emit after
      Multi commit succeeds. Measurements `%{duration, batch_size}`.
    - `[:scriba, :projection, :dead_letter]` — once per dead-lettered
      event after the Multi commits. Metadata `%{projection, position,
      stream_id, event_type, error_kind}`.
    - `[:scriba, :projection, :started | :paused | :resumed]` —
      Coordinator lifecycle events. `:started` fires once per
      Coordinator-process lifetime (Pipeline DOWN → re-up does NOT
      re-fire).
    - `[:scriba, :projection, :cache_initialized]` — ETS preload
      from Postgres at Coordinator start.
- **Dead-letter routing.** Handler `{:error, _}` returns and raised
  exceptions route to `scriba_dead_letters` after retry exhaustion,
  with the cursor advancing past the failed event (skip-and-continue
  per architecture §9.2). The dead-letter insert and the cursor
  advance commit atomically in the same Multi as the batch's
  successful writes.
- **Retry policy** with exponential backoff (default 3 attempts at
  100ms / 1s / 10s) configurable per projection via `retry:
  [max_attempts: N, backoff: [...]]` in `use Scriba.Projection`.
  `retry: false` opts out for immediate dead-letter. Implementation
  is an in-handler `Process.sleep` loop — see `SCRIBA_ARCHITECTURE.md`
  §9 for why this is the right primitive rather than
  `Process.send_after/3` against Broadway's processor model.
- **Real pause / resume** with held demand. The Coordinator's
  `:running → :paused` transition signals the source's GenStage
  producer to stop yielding; the Pipeline tree stays alive,
  in-flight events finish their commit lifecycle, accumulated
  demand drains on resume. Replaces the earlier
  `terminate_child/restart_child` implementation that was
  misleadingly named.
- **`Scriba.Telemetry.Handler`** — placeholder GenServer slot in the
  supervision tree. v0.1 has no default attaches; users attach their
  own in their app's `start/2`. The slot exists so v0.2's dashboard
  work can land into a stable supervision shape.

#### Migrations

- `Scriba.Migrations.up/0` and `down/0`. Users call these from their
  own Ecto migration:
  ```elixir
  defmodule MyApp.Repo.Migrations.AddScribaTables do
    use Ecto.Migration
    def up,   do: Scriba.Migrations.up()
    def down, do: Scriba.Migrations.down()
  end
  ```
  Creates `scriba_positions` (per-stream cursors) and
  `scriba_dead_letters`.

#### Documentation

- [`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md) — the
  architectural contract. Non-negotiable invariants, supervision tree,
  Coordinator state machine, position-tracking storage shape, error
  handling, property tests, dependencies, file layout, behaviour
  signatures.
- Module-level moduledocs on every public module documenting the
  contract from the user's perspective, including sharp edges
  (`Scriba.Source.Commanded`'s pause-memory caveat,
  `Scriba.Target.Ecto`'s `{:multi, _}` escape hatch for SQL-level
  increments, etc.).

#### Example application

- [`examples/bank/`](examples/bank) — self-contained Mix project
  demonstrating the five-line API end-to-end. Real Commanded
  (`Commanded.EventStore.Adapters.InMemory` for fast iteration),
  real Ecto, real read model. Three accounts, fifty random
  deposits/withdrawals, the projection converges and the demo
  prints the final balances. `mix bank.setup` + `mix bank.demo`
  runs in under 30 seconds against a local Postgres.

### Fixed

- **`Scriba.Source.Commanded.to_message/2` read the wrong event-struct
  field.** It read `stream_uuid` from Commanded's `RecordedEvent`, but
  the field in Commanded 1.4 is `stream_id`. Any event delivered from a
  real Commanded subscription would have raised `KeyError` on the first
  message. The bug was present in the initial `Source.Commanded`
  implementation and went undetected because that path had no end-to-end
  coverage until the Commanded integration tests were added during
  pre-release verification (commit `2778ae9`). It never shipped in a
  tagged release — fixed before v0.1.0. Anyone evaluating Scriba against
  Commanded from a pre-release snapshot who hits a `stream_uuid`
  `KeyError` should move to v0.1.0.

### Property tests

The three architectural invariants from `SCRIBA_ARCHITECTURE.md` §10
are mandatory for v0.1 and covered by property tests:

- **P1 — Per-stream ordering.** The sequence of events delivered to
  `handle/2` for any stream is a prefix of the source order for that
  stream.
- **P2 — Position monotonicity.** Committed positions in Postgres are
  monotonically non-decreasing across all observed states, including
  across simulated worker crashes (200 iterations against real
  Postgres).
- **P3 — Effectively-once under crash.** For each event, the handler's
  effect appears in the target exactly once after recovery from
  arbitrary crash schedules (100 iterations against real Postgres for
  the cursor-resume variant; the integration-test side of the
  property is in `test/scriba/projection/pipeline_test.exs`).

### Dependencies

- `:broadway` `~> 1.1`
- `:ecto_sql` `~> 3.11`
- `:postgrex` `~> 0.17`
- `:telemetry` `~> 1.2`
- `:jason` `~> 1.4`
- `:commanded` `~> 1.4` (optional)

Dev / test:
- `:stream_data`, `:ex_doc`, `:credo`, `:dialyxir`.

### Architectural decisions worth knowing

These are documented at length in `SCRIBA_ARCHITECTURE.md` but worth
calling out for users upgrading from `commanded_ecto_projections` or
similar:

- **`name` and `version` are separate fields.** Do not encode the
  version in the name (`"orders_v1"` is wrong; the macro warns at
  compile time). The architecture supports running multiple versions
  side-by-side during a cutover via two modules with different
  `version:` integers.
- **Dead-lettered events advance the cursor.** Blocking the projection
  on a bad event is the #2 complaint on ElixirForum after lag
  visibility (per architecture §9.2). Scriba defaults to
  skip-and-continue with dead-letter visibility via telemetry; users
  who need block-on-failure semantics build that on top.
- **No idempotency on `pause`/`resume`.** `pause` on `:paused` and
  `resume` on `:running` return `{:error, {:invalid_state, _}}`, not
  `:ok`. Silent idempotency hides bugs. Callers wanting "make sure
  this is paused" semantics check `Scriba.info/1` first or
  pattern-match the matching-state error case as success.
- **Multi transaction failures do not retry through the per-event
  retry policy.** Retry wraps the handler call, not the Multi commit.
  Database-level failure leaves the batch unacked; the source
  re-delivers when the projection's Pipeline next runs.

[Unreleased]: https://github.com/thatsme/scriba/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thatsme/scriba/releases/tag/v0.1.0
