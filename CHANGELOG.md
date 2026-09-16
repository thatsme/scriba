# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-16

Operational release: a projection can now say how far behind it is, be
tested without a pipeline, be inspected after it dead-letters, be rebuilt,
and survive a rolling deploy. Upgrading from 0.1.x means one migration —
`Scriba.Migrations.up(from: 1)` — and nothing else breaks.

### Changed

- Documented the assumption `:position` carries: a global, monotonic,
  numeric ordinal over every event a projection consumes. Dedup, cursor
  monotonicity, the watermark and `:start_from` all rest on it, and
  Commanded's `event_number` satisfies it — which is also why the cursor
  carries over from `commanded_ecto_projections`. A store whose position is
  not a single increasing integer (commit/prepare pairs, a vector clock) is
  a design question rather than an adapter detail, and is left open rather
  than guessed at.

- `bench/` gained a suite of experiments that need a real event store rather
  than a real database: acknowledgement loss under a straggler, standby
  takeover, the watermark end to end, subscription contention, and the
  `broadway_dashboard` integration. Each one refutes or confirms a claim the
  documentation makes, which is why they are kept rather than deleted once
  the question was answered. The contention experiment is the reason standby
  exists: it disproved a predicted crash loop and found the real gap, which
  was that nothing retried.

- Documented that the target is the transaction boundary — read-model rows,
  cursor advances and dead letters commit together or not at all — and that
  cursor and dead-letter storage will not be split into a separate
  behaviour. Splitting them would either leave them in the same transaction
  anyway or turn effectively-once delivery into at-least-once with a crash
  window.

  Other sources and targets are not being prepared for speculatively. An
  interface with one implementation behind it encodes that implementation's
  assumptions, which is how the acknowledgement defect fixed in 0.1.3
  survived a green test suite. The README and `Scriba.Target` now invite an
  issue instead: another adapter gets built with whoever needs it.

- **A producer that cannot get its subscription now stands by instead of
  failing.** A persistent subscription admits one subscriber, so on a
  multi-node deployment every node but one is refused. Previously the loser
  raised during `init`, which meant `DynamicSupervisor` never adopted it and
  `start_projection/1` returned an error — no crash loop, but no standby
  either: nothing retried, so a failover had nobody to fail over to.

  The producer now starts, retries in the background, and acquires the
  subscription when the holder releases it. The retry curve has three
  phases, because two different failures share the path: five attempts in
  milliseconds for the reap race after a deliberate producer death, then one
  a second for half a minute — a killed tree holds its names until its
  slowest in-flight handler returns — then about once a minute with jitter,
  so standbys that started together do not retry in lockstep.
  Configuration errors still raise; retrying those forever would hide them.

  Two new telemetry events make a takeover observable:
  `[:scriba, :source, :standby]` per failed attempt, and
  `[:scriba, :source, :subscribed]` when acquired. A standby's projection
  reports `:running`, because its pipeline is up — the telemetry is what
  distinguishes the node doing the work.

  This also covers a cutover from `commanded_ecto_projections`: Scriba can
  be started while the old projector still holds the subscription name, and
  picks up when it stops.

### Added

- **`REBUILDING.md` and `Scriba.reset/2`.** Rebuilding a read model is a
  procedure, not a function: declare the same `name` with a new `version`,
  start it from `:origin` with its own subscription name, watch `:lag_ms`
  fall, point reads at the new table, then retire the old version.
  `reset/2` is the last step — it clears a version's cursors and watermark
  (and dead letters on request) so it can start over, refuses while the
  projection is running, and does not touch the read model, which Scriba
  does not know the shape of.

  The guide is explicit about what will bite: handler side effects replay
  over the whole history, and dead letters are not replayed — a rebuild
  that ends with the same error kinds fixed nothing. It also records what
  Scriba will not do: shadow targets and an atomic swap, which are the
  application's decision, and progress as a percentage, which the source
  cannot supply because Commanded exposes no head position.

- **Dead-letter inspection**: `Scriba.dead_letters/2` lists rows with
  filters, paging and ordering; `Scriba.dead_letter_stats/2` summarises how
  many, of what kind, over what span. `Scriba.DeadLetter.list/3`, `count/3`
  and `stats/3` are the underlying API.

  They read the table rather than a running process, and the module form
  takes the repo from the projection's own config, so a halted or stopped
  projection can be inspected — which is when anyone actually looks.

  The `by_error_kind` distribution is the diagnosis: one kind on one stream
  is a poison event, one kind across every stream is a schema problem that
  dead-lettering is papering over.

  No replay function. `:event_data` is stored serialized — `__struct__`
  becomes a string — so a row records what failed rather than a value that
  can be re-dispatched; replay has to read the event from the source by
  position, and needs an ordering policy this does not yet have.

- README documents running `broadway_dashboard` against a projection. It
  works as-is: a projection is a Broadway topology, and the dashboard both
  discovers it through `Broadway.all_running/0` and accepts Scriba's
  `{:via, Registry, ...}` pipeline names. Scriba will not ship a dashboard of
  its own.

- `:lag_interval` set in `use Scriba.Projection` was accepted and then
  ignored — the macro validated it but never passed it on, so `lag_interval: 0`
  did not disable anything and the Coordinator always used the 5s default.
  Found by the release audit, between the option shipping and the release.

- **`[:scriba, :projection, :lag]` telemetry.** The Coordinator emits it on a
  timer — `:lag_interval`, default 5s, `0` disables — carrying `lag_ms` and
  the `watermark` it was derived from, with the projection's `status` in the
  metadata.

  On a timer rather than on traffic, deliberately: a projection that has
  stopped receiving events emits nothing else, which is exactly when an
  operator wants to know how far behind it is. It stays silent until the
  projection has committed something, because a projection with no watermark
  reporting `lag_ms: 0` would read as caught-up when it has not started.

  Throughput gets no event of its own. Broadway's batch telemetry and the
  per-event `:stop` events already carry the rate, and a second number could
  disagree with them.

- **`scriba_watermarks`: the contiguous global position a projection has
  reached**, surfaced as `:watermark` and `:lag_ms` on `Scriba.info/2`. Every
  event at or below the watermark has been committed, skipped or
  dead-lettered, with no gap below it — so it is a position a replica could
  resume from, and the number that says how far a rebuild has got. Per-stream
  cursors answer neither question: a minimum across them ignores streams the
  projection never wrote to, a maximum counts work above an event still in
  flight.

  `:lag_ms` is measured from the event's own `occurred_at`, not from the
  event store's head — Commanded's adapter behaviour exposes no head
  position, so event-count lag is not obtainable, and time-lag is what
  operators alert on anyway.

  The source already computed this number to acknowledge safely (0.1.3); this
  persists it. Written outside the commit transaction and throttled to about
  one write a second while events are in flight, flushed immediately once the
  projection catches up — so it can trail what was applied but never runs
  ahead of it, which is the only direction that is recoverable.

- Migration steps are idempotent (`create_if_not_exists`). Without that, the
  documented upgrade path broke fresh installs: an app's original migration
  calls `up()`, which means "latest", so a new database got the version 2
  table from it and then the upgrade migration collided. Found by running
  `examples/bank` end to end as part of the release gate.

- **Versioned migrations.** `Scriba.Migrations.up/1` takes `:from` and `:to`,
  so a schema change ships as a numbered step: version 1 is
  `scriba_positions` + `scriba_dead_letters`, version 2 adds
  `scriba_watermarks`. A fresh install still calls `up()`; an existing one
  adds a migration calling `up(from: 1)`. Scriba tracks no migration state of
  its own — the version lives in the user's migration file, where Ecto
  already records what ran.

- **`Scriba.Testing` — run a projection's handlers in a test without a
  pipeline.** `project/3` applies events through the projection's configured
  target and commits them, so a test asserts on read-model rows; `handle/3`
  calls one clause with no database, for asserting the shape a handler
  returns. Events run in one transaction, in order, with per-stream cursors
  advancing as they do in production, and the result reports what committed,
  skipped, failed, or came back in a shape the target cannot apply.

  It deliberately stops at the handler and the commit: no retries, no
  dead-letter routing, no dedup, no telemetry. Those decisions live in the
  pipeline, and a second implementation of them here would be the copy that
  drifts.

### Changed

- The release gate (architecture §14) now requires the CHANGELOG entry to
  account for everything in the release, checked against
  `git log <previous tag>..HEAD`, and records that a published tag is not
  moved afterwards.

## [0.1.4] - 2026-09-16

Documentation only — no code change from 0.1.3.

### Changed

- **Every shipped document audited against the code; 39 claims corrected.**
  README.md, SCRIBA_ARCHITECTURE.md, MIGRATION.md, every `@moduledoc` and
  public `@doc` in `lib/`, `examples/bank/README.md` and `bench/README.md`
  were checked claim by claim against the implementation, with no document
  treated as evidence for another. Several described the opposite of what the
  engine does:

    - A handler `:skip` does **not** advance its stream's cursor — the
      pipeline rejects every `:skip` from `stream_advances`, dedup-induced or
      not. The README and `Scriba.Target.Ecto` said it does.
    - Integrity-class commit failures **are** dead-lettered, in a transaction
      of their own, with `error_kind` `"commit:<SQLSTATE>"`. `Scriba.Target`
      and `Scriba.DeadLetter` said commit failures never are.
    - A projection also halts when **every** attempted write in a batch fails
      on integrity grounds, and `:halt_reason` is then
      `{:integrity_wipeout, n}` rather than a `Postgrex.Error`. Only
      structural errors were documented as halting.
    - `Scriba.Test.Source` requeues failed messages in position order; three
      places still said it discards them "so the suite cannot replay
      anything".
    - Events reach the dead-letter table by five paths, not two: multi-key
      collisions and unapplicable handler returns bypass the retry layer.

  Also corrected: the Coordinator state struct (11 fields, not 7), the halt
  transition (accepted from every state, not just `:running`),
  `Scriba.Info`'s `:halt_reason` field, the §8/§9 DDL
  (`:utc_datetime_usec` is `timestamp`, not `timestamptz`), P1's assertion
  (the target's commit log, not `handle/2`), two dual-emit telemetry sites,
  the §12 file layout, the `Scriba.Source` behaviour (a GenStage producer
  plus `Broadway.Acknowledger`, not `Broadway.Producer`), `:commanded`'s
  dependency grouping, the missing `:halted` case in `pause/1` and
  `resume/1`, MIGRATION.md's claim that multi-key collisions raise
  `ArgumentError`, and `bench/README.md`'s pre-watermark figures.

- **A full documentation audit is now a release gate** (architecture §14).
  Auditing only the documents a change touches is what let the above survive:
  documents drift against code they never mention.

- `mix docs` now generates without warnings. Internal modules carry
  `@moduledoc false`, so every deliberate mention of one — the telemetry
  catalog naming its emitters, the architecture guide naming processes, this
  file naming what changed — produced a "references … but it is hidden"
  warning. `:skip_code_autolink_to` in `mix.exs` lists those names, so they
  render as plain code instead of failing to link. A warning now means a
  genuinely broken reference.

- Throughput figures in the README and `Scriba.Source.Commanded` re-measured
  after the 0.1.3 acknowledgement change, on 5,000 events over 100 streams:
  9.1 events/sec at the adapter default, 6,002 with `buffer_size: 500`.

## [0.1.3] - 2026-09-16

### Fixed

- **Acknowledgement advances only across a gapless prefix of committed
  events.** Scriba acknowledged each batch as it committed. Acks are prefix
  acks — acking event 7 acknowledges everything up to 7 — and batches do not
  commit in source order when `:parallelism` exceeds 1. A handler still
  working on event 5, or sleeping in the retry loop, did not stop events 6
  and 7 from committing and acking, which moved the subscription's
  checkpoint past event 5. A crash in that window lost event 5 outright: the
  store believed it had been delivered, the per-stream cursor never
  advanced, and nothing redelivered it. No dead letter, no cursor anomaly,
  no log line, and `Scriba.info/2` still reporting `:running`.

  The producer now tracks dispatched events in delivery order and
  acknowledges only the longest run of committed ones with no gap. An
  uncommitted event holds the watermark back however many later events have
  committed; a crash then replays from below it, and pipeline-side dedup
  drops whatever had already been applied.

  **0.1.2 users should upgrade.** That release forwards `:buffer_size` and
  its documentation recommends raising it — which is exactly what opens this
  window. At the adapter default of one in-flight event, no later event can
  overtake a straggler, so the window is nearly unreachable; with
  `buffer_size: 500` it is wide.

  Reproduced against a real EventStore in `bench/test/ack_loss_test.exs`,
  which fails on 0.1.2 and passes here.

  Also faster: one acknowledgement per drain rather than one per event
  removes a `GenServer.call` per event from the commit path. The benchmark
  went from 2,448 to 4,761 events/sec at `buffer_size: 500`.

## [0.1.2] - 2026-09-16

### Added

- **`:buffer_size`, `:concurrency_limit` and `:partition_by` are forwarded to
  the event store subscription.** `Scriba.Source.Commanded` previously
  subscribed with no options at all, so the adapter's defaults always applied.
  `EventStore`'s default is one in-flight event per subscriber, and that — not
  `:parallelism` — is what bounds catch-up: Scriba acknowledges after the batch
  commits, so a batcher holding a single event waits out its full
  `:batch_timeout` before acking and releasing the next.

  Measured against a real EventStore, 5,000 events over 100 streams: 9.1
  events/sec at the adapter default, 2,448 events/sec with `buffer_size: 500`.

  Scriba sets no default of its own — the adapter's still applies unless
  configured, so this changes nothing for an existing projection until it opts
  in. Raising the buffer trades memory and redelivered-work-after-a-crash for
  throughput.

      source: {Scriba.Source.Commanded,
               application: MyApp.CommandedApp,
               buffer_size: 500}

### Fixed

- **Acknowledgement now comes from the process that holds the subscription.**
  Against `commanded_eventstore_adapter` — or any adapter that identifies the
  acking subscriber by `self()` — every acknowledgement Scriba issued was
  silently discarded. The subscription never advanced, and the projection
  stalled permanently once the event store's in-flight buffer filled. Measured
  against a real EventStore: one event projected, then nothing, with status
  still `:running`, no error, no log line.

  `ack/3` is a Broadway acknowledger, so it runs in a batch-processor process
  rather than the producer that subscribed. It called `ack_event/3` from
  there. `Commanded.EventStore.Adapters.InMemory` takes the subscription as an
  argument and ignores the caller, so acks worked; `EventStore` resolves the
  subscriber from `self()` and its subscription FSM drops acks from any pid it
  does not recognise. The engine was correct only against the adapter it was
  tested with.

  The batch processor now hands its acknowledgements to the producer, which
  issues them in delivery order. Both adapters see an ack from a pid they
  recognise. No public API, configuration, schema or telemetry change.

  Anyone running Scriba against a persistent event store should upgrade: on
  0.1.0 and 0.1.1 the projection stops after the first batch and does not
  report that it has.

## [0.1.1] - 2026-07-30

### Fixed

- **A halted projection is now queryable, not just observable at the instant
  it halts.** `Scriba.info/2` reported `:running` for a projection that had
  hit a structural commit failure and would never move again — the halt was
  announced once via `[:scriba, :projection, :halted]` and a log line, and was
  invisible from then on. An operator who was not subscribed to telemetry at
  that moment had a stopped projection and no way to see it, which is the same
  silent-stall shape the halt path exists to replace.

  The Pipeline now reports structural failures to the Coordinator, which
  carries a terminal `:halted` state. `Scriba.info/2` exposes it as `:status`
  along with a new `:halt_reason` field naming the cause (typically a
  `Postgrex.Error` whose SQLSTATE identifies it). Polling `status` is enough
  to detect a stopped projection.

  `pause/1` and `resume/1` are rejected from `:halted`; `stop/1` is the way
  out once the underlying schema or permission is fixed. Additive — no
  existing field or return value changed.

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
    - `[:scriba, :source, :batch, :failed]` — a batch failed to commit
      and nothing in it was acknowledged. Measurements `%{count}`,
      metadata `%{subscription, reason}`.
    - `[:scriba, :projection, :halted]` — a structural commit failure
      stopped the projection. Metadata `%{projection, reason, failure}`
      where `failure` is the SQLSTATE label. **The event to page on.**
- **Commit failures are classified by SQLSTATE, not guessed**
  (`Scriba.Failure`). `:integrity` (classes 22/23, `Ecto.ConstraintError`,
  invalid changesets) is deterministic and event-specific — the batch is
  re-applied one transaction per event so the offender dead-letters with
  `error_kind` `"commit:<SQLSTATE>"` and the rest commit. `:transient`
  (classes 08/53, `40001`, `40P01`, `57014`, `DBConnection` errors)
  replays. `:structural` (class 42 and anything unrecognised) halts the
  projection and emits `[:scriba, :projection, :halted]`, because
  replaying loops forever and dead-lettering would destroy a batch over a
  fixable deploy-ordering mistake. In the per-event pass a stream stops at
  its first unresolved event rather than skipping past it, preserving
  per-stream ordering and preventing a cursor gap.
- **Batch commit failures replay; they never partially acknowledge.**
  When a target's transaction does not commit, nothing in the batch is
  acknowledged — including the messages that succeeded, because event
  store acks are prefix-acks and cannot express a gap. The source stops
  its producer, which rewinds the subscription to its last durable
  checkpoint; the batch is redelivered and source-side dedup filters
  whatever did commit. See `Scriba.BatchCommitError`.
- **Malformed handler returns dead-letter instead of wedging the
  projection.** A return outside §4.2's six shapes is routed to
  `scriba_dead_letters` with `error_kind` `"invalid_return"` and the
  cursor advances. Targets declare their own vocabulary through the
  optional `c:Scriba.Target.valid_result?/1` callback, so this does not
  hard-code the Ecto target's shapes into the engine.
- **Duplicate `Ecto.Multi` operation names within a batch are detected
  before assembly.** Scriba merges a whole batch into one Multi, so two
  events naming an operation identically would collide inside
  `Repo.transaction/1` — deterministically, on every redelivery. The
  first claim now wins and later claimants dead-letter individually with
  `error_kind` `"multi_key_collision"`. Scriba's own step keys
  (`{:scriba_event, _}`, `{:scriba_position, _}`,
  `{:scriba_dead_letter, _}`) are reserved.
- **Cursor monotonicity is enforced in the schema.** The
  `scriba_positions` upsert is `GREATEST(existing, incoming)`, so a
  cursor cannot move backwards regardless of what the Pipeline computes.
- **Restart intensity is chosen, not defaulted.**
  `Scriba.Projection.Supervisor` runs at 30 restarts per 60 seconds. A
  commit failure now kills the producer on purpose, so the OTP default of
  3-in-5 would exhaust during any real outage and propagate toward the
  host application; 30-in-60 absorbs a multi-minute outage while still
  bounding a genuine crash loop.
- **Resubscribe tolerates the reap race.** After a producer dies to force
  a replay, the event store may not yet have processed its `DOWN`, so
  resubscribing can transiently see `:subscription_already_exists`. That
  is now retried with backoff (~1.5s total) and, if it persists, reported
  as a name conflict rather than as a migration problem.
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
- Scriba attaches **no** telemetry handlers of its own. It emits events
  and gets out of the way; users attach their own in their app's
  `start/2`. Default handlers, if any, are a v0.2 concern and will
  arrive with the behaviour that justifies them.

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

- [`MIGRATION.md`](MIGRATION.md) — migrating from
  `commanded_ecto_projections`. `project/2,3` → `handle/2`, the
  `Ecto.Multi` differences (one transaction per *batch*, not per event),
  the `meta` map mapping, callbacks with no equivalent
  (`after_update/3`, `schema_prefix/1`, `consistency: :strong`), and
  cursor carry-over: both libraries track Commanded's global
  `event_number`, so `last_seen_event_number` passed as `:start_from`
  cuts over in place with no read-model rebuild. `:start_from`
  exclusivity verified against `commanded_eventstore_adapter` +
  `eventstore`.
- Legacy `use Scriba.Projection` options raise with targeted guidance
  rather than a generic unknown-option error — `:consistency` most of
  all, since it is the only migration gap that is invisible at runtime.
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

- [`examples/bank/`](https://github.com/thatsme/scriba/tree/main/examples/bank)
  — self-contained Mix project
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
