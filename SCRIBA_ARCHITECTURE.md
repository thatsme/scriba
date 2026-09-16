# Scriba Architecture

> This document is the architectural contract for Scriba v0.1. It is
> prescriptive about structure, invariants, and dependencies. It is
> deliberately silent on implementation details that should follow from
> the constraints.

---

## 1. Purpose

**Scriba is a projection engine for Elixir event-sourced systems.** It
keeps read models continuously synchronized with an event stream by
pulling events from a source, routing them to user-defined `handle/2`
callbacks in per-stream order, applying results atomically with
position tracking, and emitting telemetry for operational visibility.

It is **not** an event store, a CQRS framework, a stream processing
engine, a message queue consumer, or a job queue. It composes with
Commanded; it does not replace it.

The user's mental model must remain tiny: *"I write a module with
`handle/2` clauses; Scriba calls them at the right time, in the right
order, exactly the right number of times, and tells me if anything
goes wrong."*

---

## 2. Scope of v0.1

**In scope:**

- `Scriba.Projection` behaviour with `__using__` macro
- `Scriba.Source.Commanded` adapter
- `Scriba.Target.Ecto` adapter with atomic position tracking
- Partitioned worker pool with per-stream ordering (consistent hash on `stream_id`)
- Telemetry events: `[:scriba, :projection, :event, :start | :stop | :exception]`
- Position tracking table + Ecto migration
- Supervision tree
- Property-based tests for the three core invariants (see §10)
- Dead-letter handling for failing events (see §9)
- Hex-publishable as `0.1.0` with README, CHANGELOG, LICENSE (Apache-2.0)

**Out of scope for v0.1** (do not build, do not stub, do not "leave room for"):

- LiveView dashboard (v0.2)
- Lag/throughput metrics beyond raw telemetry events (v0.2)
- Online rebuild, shadow targets, swap (v0.3)
- Adapters other than Commanded source + Ecto target (v0.4)
- Multi-target fan-out (v0.4)
- Backpressure tuning knobs beyond Broadway defaults (v0.5). Note: since
  0.1.2 the source forwards `:buffer_size`, `:concurrency_limit` and
  `:partition_by` to the event store subscription. That is pass-through to
  the adapter, not a knob Scriba implements, and it was added because the
  adapter default bounds catch-up at roughly 10 events/sec.

---

## 3. Non-negotiable design rules

These override any other consideration. If an implementation choice
violates one of these, the choice is wrong.

1. **Correctness over throughput.** Position update is atomic with
   read-model write inside a single `Ecto.Multi`. No exceptions.
2. **Per-stream ordering is preserved.** All events for a given
   `stream_id` route to the same worker via consistent hashing.
   Within a worker, events are processed serially.
3. **No clever metaprogramming.** The `__using__` macro generates a
   thin module. No DSL. No AST manipulation beyond what `defmacro`
   gives you naturally.
4. **Boring, supervised OTP.** A projection is a module. A worker is a
   process under a known supervisor. The supervision tree is
   inspectable in `:observer` and tells the truth about runtime
   structure.
5. **No magic dependencies.** Deps for v0.1 are listed in §11. Adding
   one requires justification. Removing one is fine.
6. **The five-line API is the contract.** See §4. If a feature
   requires breaking that API, the feature is wrong, not the API.

---

## 4. Public API surface (frozen for v0.1)

### 4.1 The user-facing projection module

```elixir
defmodule MyApp.Projections.Orders do
  use Scriba.Projection,
    name: "orders",
    source: {Scriba.Source.Commanded, application: MyApp.CommandedApp},
    target: {Scriba.Target.Ecto, repo: MyApp.Repo},
    parallelism: 16

  def handle(%OrderPlaced{} = event, _meta) do
    {:insert, %OrderReadModel{
      id: event.order_id,
      customer_id: event.customer_id,
      status: "pending"
    }}
  end

  def handle(%OrderShipped{order_id: id}, _meta) do
    {:update, OrderReadModel, [id: id], set: [status: "shipped"]}
  end

  def handle(_event, _meta), do: :skip
end
```

`:version` defaults to `1` and is usually omitted; `:partition_by`
defaults to `:stream_id` (the only value supported in v0.1).
`use Scriba.Projection` enforces required options at compile time and
emits a warning if `:name` matches the legacy `"_v<integer>"` suffix
pattern (per §5, version belongs in its own option, not in the name).

### 4.2 Handler return contract

A handler must return one of:

- `{:insert, schema_struct}` — insert one row
- `{:update, schema_module, filter_keyword, [set: keyword]}` — update by filter
- `{:delete, schema_module, filter_keyword}` — delete by filter
- `{:multi, ecto_multi}` — user-supplied `Ecto.Multi` for arbitrary work
- `:skip` — event acknowledged, no side effect
- `{:error, reason}` — explicit failure (routes to dead-letter, see §9)

**Raising an exception is also valid** and is treated as `{:error, exception}`.

The engine wraps the returned operation in an `Ecto.Multi` together
with the position update, then calls `Repo.transaction/1`. If the
transaction fails, the event goes to dead-letter; the worker continues.

#### The `meta` map

The second argument to `handle/2` is a map with these keys:

| Key | Type | Provenance | Purpose |
|---|---|---|---|
| `:id` | `String.t()` | Source-adapter-supplied | Stable event identifier (Commanded UUID, ExESDB id, etc.). Use for idempotency keys, dead-letter correlation, audit trails. **Globally unique** in well-formed event streams. |
| `:stream_id` | `String.t()` | Source-adapter-supplied | Aggregate / partition identifier. Events with the same `stream_id` route to the same processor and are handled in source order (§3 rule 2). |
| `:position` | `non_neg_integer()` | Source-adapter-supplied | Event ordering. Scriba uses this internally for ordered consumption — for cursor tracking, source-side dedup, and `safe_position`. **Not** a stable identifier; do not use for idempotency keys. |
| `:type` | `String.t()` | Source-adapter-supplied | Event type name (e.g. `"OrderPlaced"`). Useful for telemetry filtering or routing within a single `handle/2` clause. |
| `:metadata` | `map()` | Source-adapter-supplied (may be `%{}`) | Arbitrary per-event metadata from the source — correlation IDs, causation IDs, user info, etc. Pass-through; Scriba does not interpret it. |
| `:occurred_at` | `DateTime.t()` | Source-adapter-supplied | Event-time timestamp from the source. Use for time-windowed projections; do not assume monotonicity across the source. |

`:id` and `:position` answer different questions. `:id` answers "is this the
same event I saw before?" (idempotency). `:position` answers "where is this
event in the stream?" (ordering). They are independent: replays and crash
recovery may re-deliver the same `:id` at the same `:position`; partitioning
or out-of-order delivery can never re-deliver the same `:id` at a different
`:position` because in well-formed event streams `:id` is globally unique
and `:position` is determined by the source.

### 4.3 Top-level functions

```elixir
Scriba.start_projection(MyApp.Projections.Orders)
Scriba.start_projection(MyApp.Projections.Orders, parallelism: 32)   # runtime override
Scriba.pause(MyApp.Projections.Orders)
Scriba.resume(MyApp.Projections.Orders)
Scriba.stop(MyApp.Projections.Orders)
{:ok, info} = Scriba.info(MyApp.Projections.Orders)
projections = Scriba.list()
```

Every lifecycle function accepts either a projection module (reads
identity from the module's compile-time `__scriba_config__/0`) or a
string name with implicit version 1 (`Scriba.pause("orders")`) or
explicit `(name, version)` (`Scriba.pause("orders", 2)`). The module
form is refactor-safe; the string form is for operator workflows that
only know the projection's name.

`Scriba.start_projection/2`'s overrides keyword **rejects `:name` and
`:version`** with `ArgumentError` — identity is compile-time. To run a
new projection with a different version, declare a separate module
with `version: 2`. Identity-overriding at runtime would silently
create a different projection, almost always a bug.

`Scriba.info/1` returns a `Scriba.Info` struct with `:name`, `:version`,
`:status`, `:source`, `:target`, `:safe_position`, `:stream_positions` and
`:halt_reason` (the cause when `:status` is `:halted`, `nil` otherwise).
`:stream_positions` becomes `:truncated` above 1,000 streams. Lag and throughput are NOT in v0.1 — they live in
telemetry consumers.

`Scriba.list/0` returns `[%{name, version, state}]` for projections
currently registered in `Scriba.Registry`, including those in
`:stopped` state.

There is no `rebuild`, `swap!` or `reset` function. Rebuilding is not a
library operation in v0.1: a new `version` running side-by-side (§5) is
the mechanism, and cutting over is the application's choice of which
read model to query.

---

## 5. Naming: name vs version

This is a deliberate departure from the `"orders_v1"` string-suffix
pattern used by `commanded_ecto_projections`.

- `name` is the **logical projection identity**. String. e.g. `"orders"`.
- `version` is an **integer**, default `1`. Bumping it creates a
  separate projection that runs side-by-side with the old one until
  the user cuts over.

Position is tracked per `(name, version)` pair. Telemetry tags carry
both. The dashboard groups by name and shows versions as siblings.

---

## 6. Supervision tree

```
Scriba.Application
└── Scriba.Supervisor (one_for_one)
    ├── Scriba.Registry (Registry, keys: :unique — public addresses)
    ├── Scriba.Internals.Registry (Registry, keys: :unique — Broadway internals)
    └── Scriba.Projections.Supervisor (DynamicSupervisor)
        └── per projection (added via Scriba.start_projection/1):
            └── Scriba.Projection.Supervisor (rest_for_one)
                ├── Scriba.Projection.Coordinator (gen_statem)
                └── Scriba.Projection.Pipeline (Broadway)
```

The top-level supervisor also creates two shared ETS tables before
starting any child: the position cache (§8.3) and `Scriba.Circuit`'s
per-projection failure state, which must outlive a producer that dies
deliberately on commit failure (§9). Both are owned by the supervisor
process and live for the application's lifetime.

### 6.1 Why these choices

- **`Registry` with unique keys.** Lookup of coordinator pid by
  projection name. Standard.
- **`DynamicSupervisor` at the projections layer.** Projections are
  added at runtime via `start_projection/1`. They are not declared at
  compile time in the application's supervision tree.
- **`rest_for_one` for the per-projection subtree.** If the
  Coordinator dies (state corruption), the pipeline must restart
  with it. If the pipeline dies, the Coordinator survives — its
  internal lifecycle state is intact.
- **`gen_statem` for the Coordinator** with `:handle_event_function`
  callback mode. Lifecycle is genuinely a state machine:
  `:initializing → :running → :paused → :draining → :stopped`, plus the
  terminal `:halted`. State-entry hooks via `enter` events keep pipeline
  start/stop logic clean. See §7.
- **No singleton `Scriba.PositionStore` GenServer.** Position lives in
  Postgres (authoritative) with one shared ETS cache keyed by
  `{name, version, stream_id}`. See §8. A mediating process would be a
  bottleneck on the commit path.

### 6.2 Process registration

Coordinators register as `{:via, Registry, {Scriba.Registry, {:coordinator, name, version}}}`.
Broadway pipelines register as `{:via, Registry, {Scriba.Registry, {:pipeline, name, version}}}`.
Per-projection supervisors register as
`{:via, Registry, {Scriba.Registry, {:projection_supervisor, name, version}}}`.
Broadway's own internal processes use `Scriba.Internals.Registry` instead,
so the public registry stays readable.

### 6.3 Telemetry event surface (v0.1)

Twelve events fire. `Scriba.Telemetry`'s moduledoc is the catalog users
read; this table is the same surface, and the two are kept in step. Lag
and throughput events are explicitly out of scope (see §2) — do not add
them here without amending that section.

| Event | Emitter | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:scriba, :projection, :event, :start]` | `handle_message/3` via `:telemetry.span/3` | `monotonic_time`, `system_time` | `projection`, `event_type`, `stream_id`, `position`, `telemetry_span_context` |
| `[:scriba, :projection, :event, :stop]` | same | `duration`, `monotonic_time` | same as `:start` |
| `[:scriba, :projection, :event, :exception]` | same | `duration`, `monotonic_time` | start metadata + `kind`, `reason`, `stacktrace` |
| `[:scriba, :projection, :batch, :stop]` | `handle_batch/4` — two sites: the whole-batch commit, and the per-event fallback pass once it resolves everything | `duration`, `batch_size` | `projection` |
| `[:scriba, :projection, :dead_letter]` | `handle_batch/4`, one per dead-lettered event after the Multi commits — both from the batch path and from the per-event fallback, which is the usual route for commit failures | `system_time` | `projection`, `position`, `stream_id`, `event_type`, `error_kind` |
| `[:scriba, :projection, :started]` | `Scriba.Projection.Coordinator` (first `:initializing → :running`, fires once per Coordinator-process lifetime; Pipeline DOWN→re-running does NOT re-fire) | `system_time` | `projection` |
| `[:scriba, :projection, :paused]` | `Scriba.Projection.Coordinator` (on `:running → :paused`, after source pause signal sent) | `system_time` | `projection` |
| `[:scriba, :projection, :resumed]` | `Scriba.Projection.Coordinator` (on `:paused → :running`, after source resume signal sent) | `system_time` | `projection` |
| `[:scriba, :projection, :event, :skipped]` | `handle_message/3` (no handler ran) | `system_time` | `projection`, `reason` (`:dedup` or `:handler`), `event_type`, `stream_id`, `position` |
| `[:scriba, :projection, :cache_initialized]` | `Scriba.Position.init_cache/3` | `wiped_count`, `preloaded_count` | `name`, `version`, `source` |
| `[:scriba, :source, :batch, :failed]` | the source's acknowledger (a batch did not commit; nothing was acknowledged) | `count` | `subscription`, `reason` |
| `[:scriba, :projection, :halted]` | `halt_batch/3`, from `handle_batch/4` (structural commit failure; the projection has stopped making progress) | `system_time` | `projection`, `reason`, `failure` (SQLSTATE label) |

Conventions:

- `projection` is `%{name: String.t(), version: pos_integer()}`.
- `event_type` is `event.type` (source-adapter-supplied; e.g. `"OrderPlaced"`).
- The exception event uses Erlang's span shape — `kind` / `reason` /
  `stacktrace` in **metadata**, not measurements. `:telemetry.span/3`
  re-raises after emission, so Broadway still observes the failure and
  marks the message. Retry and dead-letter routing wrap **outside** this
  span, intercepting before Broadway's default failure path.
- Neither dedup nor a `:skip` handler return emits the per-event span —
  there was no handler call to measure. Both emit
  `[:scriba, :projection, :event, :skipped]` instead, distinguished by
  `reason` (`:dedup` or `:handler`). Skip is the only outcome that leaves
  no other trace, so without this event the conservation identity
  `delivered == rows + dead letters + skipped` cannot be closed.
- Batch `:stop` is emitted only on the success branch (Multi committed).
  There is no batch-failure counterpart: failure is observable through
  `[:scriba, :source, :batch, :failed]` and `[:scriba, :projection, :halted]`,
  which say different things (§9). `batch_size` counts messages Broadway
  saw, including those whose handler returned `:skip` — they consumed
  pipeline capacity.
- Two events come from outside the Pipeline and Coordinator:
  `[:scriba, :projection, :cache_initialized]` fires from
  `Scriba.Position.init_cache/3` (`source` is `:postgres` or `:empty`), and
  `[:scriba, :source, :batch, :failed]` fires from the source's
  acknowledger, because that is where the consequence lands.

---

## 7. The Coordinator state machine

`Scriba.Projection.Coordinator` is a `gen_statem` with callback mode
`[:handle_event_function, :state_enter]`.

### 7.1 States

- `:initializing` — coordinator started; polling for Pipeline producer
  registration. Brief transient state; transitions to `:running`
  automatically once the producer is up.
- `:running` — Pipeline is live, producer is monitored, events flow
  source → processors → batchers → target.
- `:paused` — source has been signaled to stop yielding new events.
  Pipeline tree stays alive (processors, batchers, target state all
  intact); in-flight events finish their commit lifecycle. Coordinator
  alive; position frozen modulo in-flight settle.
- `:draining` — pipeline received stop signal, finishing in-flight batch
- `:stopped` — terminal; supervisor will not restart
- `:halted` — terminal. A batch failed with a structural error (§9), which
  neither replay nor dead-lettering can resolve. The Pipeline tree stays
  alive but nothing is acknowledged and no cursor moves. The state carries
  the cause, which `Scriba.info/2` exposes as `:halt_reason`.

### 7.2 Transitions

```
:initializing -- producer registered --> :running
:running      -- pause                --> :paused
:paused       -- resume               --> :running
:running      -- stop                 --> :draining --> :stopped
:paused       -- stop                 --> :stopped     (direct terminate)
:running      -- Pipeline DOWN        --> :initializing (rest_for_one respawn)
any           -- structural failure   --> :halted       (terminal; the
                                         halt cast is accepted from every
                                         state except :halted itself)
:halted       -- stop                 --> :stopped
any           -- crash                --> (supervisor restarts to :initializing, then auto-:running)
```

`pause` and `resume` are rejected from `:halted` with
`{:error, {:invalid_state, :halted}}`. `stop` is the way out, once the
schema or permission that caused the halt has been fixed.

`:running → :paused` is a **held-demand** transition, not
stop-and-restart. The Coordinator calls `Source.pause/1` on the
Broadway producer pid — the source flips a `paused: true` flag in its
GenStage state and accumulates demand without dispatching. The Pipeline
tree (processors, batchers, target state) stays alive. Resume reverses
the signal; accumulated demand drains from the source's queue.

Illegal command/state combos return `{:error, {:invalid_state, state}}`
with no idempotency — `pause` on `:paused` and `resume` on `:running`
are errors, not no-ops. Callers wanting idempotent semantics check
`Scriba.info/2` first or pattern-match the matching-state error case
as success.

### 7.3 State data

Keep the Coordinator's state struct **small**:

```elixir
%{
  name: String.t(),
  version: pos_integer(),
  source_spec: tuple(),
  target_spec: tuple(),
  handler: module(),
  parallelism: pos_integer(),
  repo: module(),
  supervisor_pid: pid() | nil,
  pipeline_pid: pid() | nil,
  pipeline_ref: reference() | nil,
  started: boolean(),          # gates once-per-lifetime :started telemetry
  halt_reason: term() | nil    # surfaced by Scriba.info/2 as :halt_reason
}
```

Position lives in Postgres + ETS. Metrics live in `:counters`.
Neither belongs in the Coordinator.

### 7.4 Pipeline supervision

The Coordinator does NOT supervise the Broadway pipeline directly. The
pipeline is a sibling under the projection's `rest_for_one`
supervisor. The Coordinator monitors the pipeline pid via
`Process.monitor/1` for observation only — restart is the supervisor's
job, not the Coordinator's.

### 7.5 Periodic timers — `state_timeout` is the wrong primitive

This note is load-bearing for v0.2's lag/throughput work. Recording it
here so the implementation does not re-discover it under load.

`gen_statem`'s `state_timeout` action **resets on every event in that
state**. The Coordinator already uses `state_timeout` for the
pipeline-pid lookup poll (one-shot, re-armed only when the lookup
returns `:pending`) — that pattern is correct because the timer is
event-driven, not periodic.

A **periodic** timer (e.g. "emit lag every 1s") cannot use
`state_timeout`: under any non-trivial event load in `:running`, the
timer is reset before it fires and the emission silently stops. This
fails open in tests (low event volume → timer fires) and fails closed
in production (high event volume → timer never fires). The kind of
subtle bug that hides for months.

For periodic timers in the Coordinator, use either:

  - `Process.send_after(self(), :tick, interval_ms)` self-message,
    re-armed in the `:info` handler, OR
  - `:erlang.start_timer/3` with explicit reference tracking.

Both are independent of state-event flow. The send_after pattern is
shorter and matches how Broadway emits its own periodic events.

---

## 8. Position tracking

### 8.1 Authoritative storage: Postgres

```sql
CREATE TABLE scriba_positions (
  projection_name varchar(255) NOT NULL,
  projection_version int NOT NULL,
  stream_id varchar(255) NOT NULL,
  position bigint NOT NULL,
  updated_at timestamp(6) NOT NULL,   -- Ecto :utc_datetime_usec
  PRIMARY KEY (projection_name, projection_version, stream_id)
);
```

The cursor is **per stream**, not per projection: per-stream ordering is
the guarantee (§3), so each stream carries its own position and a slow
stream never holds back a fast one. A secondary index on
`(projection_name, projection_version)` serves the whole-projection reads.

The Ecto migration is provided by `Scriba.Migrations.up/0` and
`down/0`. Users invoke it from their own migration file.

### 8.2 Atomic update with read-model write

Every event commit looks like:

```elixir
Multi.new()
|> apply_handler_results(handler_returns)          # user's intent, one step per event
|> Scriba.Position.multi(...)                      # one {:scriba_position, stream_id} step per stream
|> Repo.transaction()
```

Each cursor step is an upsert, not an update, and the new value is
`GREATEST(existing, incoming)`:

```sql
INSERT INTO scriba_positions
  (projection_name, projection_version, stream_id, position, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (projection_name, projection_version, stream_id)
DO UPDATE SET position = GREATEST(scriba_positions.position, EXCLUDED.position),
              updated_at = EXCLUDED.updated_at
```

Monotonicity is enforced by the database rather than by the pipeline, so a
redelivered older event cannot move a cursor backwards no matter what order
batches commit in.

If the transaction fails, neither the read-model write nor the
position update is applied. The event is retried (via Broadway
re-delivery) or routed to dead-letter after N failures.

### 8.3 ETS cache layer

**One shared table for the whole BEAM**, named `Scriba.Position.Cache` and
created by the top-level supervisor, which owns it for the application's
lifetime. Shared rather than per-projection because a per-projection table
needs a per-projection atom, and atoms are never garbage-collected — a
system that starts and stops projections dynamically would leak them.
Rows are keyed by `{name, version, stream_id}`, so projections do not see
each other's entries.

```elixir
:ets.new(Scriba.Position.Cache, [
  :set, :public, :named_table,
  {:write_concurrency, true},
  {:read_concurrency, true},
  {:decentralized_counters, true}
])
```

Workers update the cache after a successful commit. The cache is **not
authoritative** — it is a hot-read optimization for `Scriba.info/2` and for
source-side dedup. `Scriba.Position.init_cache/3` wipes and preloads one
projection's entries once per Coordinator-process lifetime, from the
Coordinator's `init/1`, so that pause → resume preserves the cache that
dedup depends on while a Coordinator crash rebuilds it from Postgres.
The preload is capped at 10,000 rows; streams beyond the cap fall back to
a lazy read on first use.

### 8.4 No PositionStore process

There is no GenServer mediating position reads or writes. Workers
write directly to Postgres (inside their Multi) and to the ETS cache.
Readers (info, telemetry) read from ETS first, fall back to Postgres
on miss.

---

## 9. Error handling and dead-letter

A commit can fail three ways, and they do **not** share a response:

1. The handler returns `{:error, reason}` — explicit. Retried per §9.1,
   then dead-lettered.
2. The handler raises — caught, converted to `{:error, exception}`, and
   treated as case 1.
3. The Multi transaction fails — a database-level error affecting the whole
   batch. Never dead-lettered on the strength of the batch failure alone:
   `Scriba.Failure` classifies the SQLSTATE and the engine picks a response
   that terminates —

   - `:transient` (classes 08, 53, 57, serialization failures, deadlocks) —
     nothing is acknowledged, the producer dies, the subscription rewinds and
     the batch is replayed with backoff.
   - `:integrity` (classes 22 and 23, constraint errors, invalid changesets)
     — deterministic and specific to one event, so the batch is retried
     per-event and the offending events are dead-lettered while the rest
     commit. Two outcomes qualify that: if any event is left unresolved the
     batch is *replayed* rather than partially acknowledged (committed work
     stands, dedup filters it on redelivery), and if **every** attempted
     write failed on integrity grounds, `Scriba.Circuit` reads that as a
     schema the handler no longer matches and halts rather than draining the
     stream into `scriba_dead_letters` over an empty read model.
   - `:structural` (class 42 and anything unrecognised) — schema or
     permissions do not match the code, which no replay can fix. The
     projection halts (§7.1) and stays loud.

Guessing from "did some events succeed?" is what this replaces: partial
success measures uniformity, not determinism, and it is wrong in both
directions.

Dead-lettered events are written to:

```sql
CREATE TABLE scriba_dead_letters (
  id bigserial PRIMARY KEY,
  projection_name varchar(255) NOT NULL,
  projection_version int NOT NULL,
  position bigint NOT NULL,
  stream_id varchar(255),
  event_type varchar(255),
  event_data jsonb NOT NULL,
  error_kind varchar(64) NOT NULL,
  error_message text,
  error_stacktrace text,
  occurred_at timestamp(6) NOT NULL DEFAULT now()   -- Ecto :utc_datetime_usec
);
```

### 9.1 Retry policy

Default: 3 attempts with exponential backoff (100ms, 1s, 10s) before
dead-lettering. Configurable per projection via
`retry: [max_attempts: N, backoff: [...]]`. `retry: false` opts out —
one attempt, immediate dead-letter on failure.

**What retries:** `{:error, _}` handler returns AND raised exceptions
(per §4.2 "Raising is treated as `{:error, exception}`"). Same backoff
schedule for both.

**What doesn't retry:** Multi-transaction failures (case 3 in §9 above)
leave the batch unacked; the source re-delivers later. From the retry
policy's perspective this is "batch never started" — the per-event
retry counter doesn't increment for case-3 failures.

**Implementation:** in-handler retry loop with `Process.sleep/1`
between attempts. Sleeping inside `handle_message` blocks ONE processor
(per-partition); other processors continue independently, and the
batcher keeps shipping batches via `batch_timeout`. Per-stream ordering
is preserved within the stuck processor's partition. Pipeline restart
during sleep resets the retry counter to 0 via source re-delivery from
the durable cursor — no per-message retry state to persist. This is the
right primitive (vs `Process.send_after/3`, which doesn't fit
Broadway's processor model).

**Backoff list semantics:** entries are the sleeps BETWEEN attempts.
N attempts need N-1 sleeps, so `length(backoff) >= max_attempts - 1`
is validated at projection start. Default `[100, 1000, 10000]`
over-provisions one entry — the third is unused at default
`max_attempts: 3` but available if `max_attempts` is bumped to 4
without overriding backoff.

**Telemetry:** each retry attempt re-invokes `:telemetry.span/3`,
producing its own `:event :start` / `:event :stop` / `:event :exception`
triple. Operators counting `:event :start` events per `event_id` can
detect retry activity. No dedicated `:event :retry` event in v0.1.

**Dead-letter after exhaustion:** the final failure result (original
`{:error, _}` or `{:exception, _, _}`) is what routes to dead-letter,
unchanged. No "retry_exhausted" wrapper — `error_kind` reflects the
actual failure cause. The retry layer is transparent to dead-letter
routing (§9 case 1 and case 2 paths).

### 9.2 The crucial choice: skip and continue

When an event is dead-lettered, the position **advances past it**.
This is deliberate. The alternative — blocking the projection until
the bad event is resolved — is the #2 complaint on ElixirForum after
lag visibility. Users can replay dead-letters manually via a function
provided in v0.2.

Document this clearly in the README. It's a sharp edge but the right
default.

### 9.3 Telemetry on dead-letter

Emit `[:scriba, :projection, :dead_letter]` with metadata
`{name, version, position, stream_id, event_type, error_kind}` so
operators can alert on it.

---

## 10. Property tests

P1, PD2 and PD3 are `StreamData` properties; the §10.4 tests are plain
ExUnit cases. P1 needs no database; everything in `test/property_db/` does,
and is excluded when `SCRIBA_TEST_DB_*` is unset. `mix test.fast` excludes
all of them.

### 10.1 P1 — Per-stream ordering

`test/property/ordering_test.exs`, 1,000 runs, no database.

For any stream S, the events that reach the target do so in source order.
Generator: events with monotonic per-stream sequence numbers, multiple
streams interleaved. Assertion: the positions the target committed, grouped
by stream, are sorted — read from the test target's commit log rather than
from `handle/2`, so it measures what was durably applied.

### 10.2 PD2 — Position consistency

`test/property_db/pd2_position_consistency_test.exs`, 200 runs, real Postgres.

Every read-model row has a `scriba_positions` row whose cursor is ≥ that
event's position — the read model can never drift ahead of the cursor. This
is what the Multi atomicity of §8.2 buys, asserted against a real database
rather than a double.

### 10.3 PD3 — Cursor resume

`test/property_db/pd3_cursor_resume_test.exs`, 100 runs, real Postgres.

Two phases per iteration: run a projection to completion, then start a new
one with the same `(name, version)` and `:start_from` set to the committed
cursor. Source-side filtering must drop every event at or below it, so the
read model and `scriba_positions` are untouched by the second start. This is
the resume-after-restart guarantee.

### 10.4 Supporting real-Postgres tests

- `e3_fault_injection_test.exs` — induces real Postgres failures (constraint
  violation, missing column) and asserts the whole path: error → classifier →
  pipeline response → dead-letter row or halt, with the conservation identity
  `events in == read-model rows + dead letters + skipped` held throughout.
- `multi_key_collision_test.exs` — a batch merged into one `Ecto.Multi`
  collides on duplicate step keys where the per-event transactions of
  `commanded_ecto_projections` did not.
- `sandbox_harness_test.exs` — foundation test for the shared-mode Ecto
  sandbox the others depend on.

Effectively-once under injected crash schedules is **not** covered by a
property test. The guarantee is enforced structurally — read-model primary
key, Multi atomicity, PD2, PD3, and pipeline-side dedup — and the crash
injection that would re-verify their composition is designed but deferred
(`docs/post-v0.1.md`).

---

## 11. Dependencies

```elixir
defp deps do
  [
    {:broadway, "~> 1.1"},
    {:ecto_sql, "~> 3.11"},
    {:postgrex, "~> 0.17"},
    {:telemetry, "~> 1.2"},
    {:jason, "~> 1.4"},

    # Optional at runtime, all environments — only Scriba.Source.Commanded
    # needs it, and that module guards with Code.ensure_loaded?/1.
    {:commanded, "~> 1.4", optional: true},

    # dev/test only
    {:stream_data, "~> 1.0", only: [:dev, :test]},
    {:ex_doc, "~> 0.31", only: :dev, runtime: false},
    {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
    {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
  ]
end
```

Notes:
- `:commanded` is `optional: true` — Scriba should compile and run
  without it. Only `Scriba.Source.Commanded` requires it; that
  module guards with `Code.ensure_loaded?/1`.
- No `:gen_state_machine`. Use Erlang's built-in `:gen_statem`
  directly. One less dep.
- No metrics library. Telemetry events only — attaching them to
  `telemetry_metrics` or anything else is the consumer's choice, not a
  dependency Scriba imposes.

---

## 12. File layout

```
scriba/
├── .credo.exs
├── .env.local.example
├── .formatter.exs
├── .gitignore
├── CHANGELOG.md
├── LICENSE                       # Apache-2.0
├── MIGRATION.md                  # ships in the package
├── README.md
├── SCRIBA_ARCHITECTURE.md        # this file; ships in the package
├── docker-compose.yml            # dev Postgres on 5433
├── docker/
├── docs/
│   └── post-v0.1.md              # deferred design notes (repo only)
├── config/
│   ├── config.exs
│   └── test.exs                  # SCRIBA_TEST_DB_* gating
├── mix.exs
├── mix.lock
├── lib/
│   ├── scriba.ex                 # public API: start_projection, pause, resume, stop, info, list
│   ├── scriba/
│   │   ├── application.ex
│   │   ├── supervisor.ex
│   │   ├── registry.ex
│   │   ├── event.ex              # %Scriba.Event{} — the engine's event shape
│   │   ├── projection.ex         # the __using__ macro + behaviour
│   │   ├── projections/
│   │   │   └── supervisor.ex     # DynamicSupervisor
│   │   ├── projection/
│   │   │   ├── supervisor.ex     # rest_for_one per projection
│   │   │   ├── coordinator.ex    # gen_statem
│   │   │   └── pipeline.ex       # Broadway module
│   │   ├── source.ex             # behaviour
│   │   ├── source/
│   │   │   └── commanded.ex
│   │   ├── target.ex             # behaviour
│   │   ├── target/
│   │   │   ├── ecto.ex
│   │   │   └── test.ex           # in-memory, for property tests
│   │   ├── position.ex           # functions over Postgres + ETS, no process
│   │   ├── dead_letter.ex
│   │   ├── failure.ex            # SQLSTATE → :transient | :integrity | :structural
│   │   ├── circuit.ex            # per-projection failure state, outlives the producer
│   │   ├── info.ex               # %Scriba.Info{} — what Scriba.info/2 returns
│   │   ├── partitioner.ex        # consistent hash logic
│   │   ├── telemetry.ex          # event-catalog moduledoc; no runtime code
│   │   ├── migrations.ex         # up/0, down/0 for users to call
│   │   └── errors.ex             # Scriba.BatchCommitError
└── test/
    ├── test_helper.exs
    ├── scriba_test.exs
    ├── support/                    # repo, migrations, generators, test doubles
    ├── scriba/                     # unit tests, mirroring lib/
    ├── property/
    │   └── ordering_test.exs       # P1 — DB-free, StreamData
    └── property_db/                # real-Postgres suite, tagged :property_db
        ├── pd2_position_consistency_test.exs
        ├── pd3_cursor_resume_test.exs
        ├── e3_fault_injection_test.exs
        ├── multi_key_collision_test.exs
        └── sandbox_harness_test.exs
```

Two test trees, deliberately: `test/property/` is DB-free, while
`test/property_db/` needs a live Postgres and is excluded when
`SCRIBA_TEST_DB_*` is unset (see README). Both are excluded from
`mix test.fast`, which is the DB-free development loop; `mix test.all`
runs everything the environment allows. `bench/` and `examples/` are
separate Mix projects and are not part of the published package.

Do not add files outside this layout without justification. Do not
create empty placeholder files for future versions.

---

## 13. Behaviours

### 13.1 `Scriba.Source`

```elixir
@callback child_spec(opts :: keyword()) :: Supervisor.child_spec()
@callback start_link(opts :: keyword()) :: GenServer.on_start()

# Held-demand pause: the producer stops dispatching and accumulates
# demand. The Pipeline tree stays alive (§7.2).
@callback pause(producer_pid :: pid()) :: :ok
@callback resume(producer_pid :: pid()) :: :ok

# Source modules are GenStage producers that Broadway starts, and they
# implement Broadway.Acknowledger. They yield events as
# %Broadway.Message{data: %Scriba.Event{}, acknowledger: ...}
```

`Scriba.Event` struct:

```elixir
%Scriba.Event{
  id: String.t(),            # globally unique event id
  stream_id: String.t(),     # used for partitioning
  type: String.t(),          # event type name
  data: term(),              # decoded event struct
  metadata: map(),
  position: non_neg_integer(),  # global stream position
  occurred_at: DateTime.t()
}
```

#### Resume semantics on Pipeline restart

When the Pipeline is restarted — through `Coordinator.pause/2` +
`Coordinator.resume/2`, or as part of `rest_for_one` recovery from a
Pipeline-or-source crash — Broadway's tree is torn down and rebuilt,
and a fresh source process is spawned via the source's `start_link/1`.

**The new source starts with whatever resume point its adapter
computes on init.** For the in-process Test source
(`test/support/test_source.ex`), the resume point is the `:start_from`
opt, which defaults to `0` — i.e. replay-from-zero on every Pipeline
restart unless the caller explicitly passes a cursor.

For production sources, the resume point should come from the source's
own server-side subscription state, not from Scriba:

* `Scriba.Source.Commanded` subscribes with a stable subscription `name`
  and a configurable `:start_from` (default `:origin`). Commanded's event
  store persists that subscription's acked position; on Pipeline restart,
  the re-subscription resumes from the persisted position.

  Two rules govern how that position advances, and both are load-bearing:

  1. **The producer acknowledges, not the batch processor.** An event store
     may resolve the acking subscriber from `self()` and discard an ack from
     any other process, without an error.
  2. **Only a gapless prefix is acknowledged.** Acks are prefix acks and
     batches commit out of order under `:parallelism > 1`, so an event still
     inside its handler holds the watermark back regardless of how many
     later events have committed. Acknowledging per committed batch instead
     checkpoints past an uncommitted event, and a crash there loses it
     silently — verified against a real event store, not reasoned about.
* Future adapters (e.g. ExESDB) follow the same pattern — server-side
  subscription state is the source of truth for "where this projection
  has consumed up to."

Pipeline-side source dedup is the
correctness safety net independent of the source adapter's resume
behaviour: even when a source redelivers events whose position is at
or below the projection's committed cursor (e.g. because the source
restarted before its own ack reached the server), the Pipeline's
`handle_message/3` checks `Scriba.Position.cache_get/4` and returns
`:skip` for any already-committed event. The handler is not invoked,
the read model is untouched, the cursor does not advance.

The two layers compose deliberately:

1. **Adapter-level resume** minimizes redelivery cost — server-side
   subscriptions mean only events past the cursor are sent over the
   wire.
2. **Pipeline-side dedup** is the correctness guarantee — even a
   buggy or naive adapter that replays from zero on every restart
   cannot violate exactly-once at the read model.

PD3 (real-Postgres property test, 100 iterations) verifies adapter-level
resume end-to-end against the Test source's `:start_from` filter.
The fast-suite integration test in
`test/scriba/projection/pipeline_test.exs` verifies Pipeline-side dedup
by terminating and restarting the Pipeline child through its supervisor
(which takes the source with it) and observing that the new source's
replay-from-zero produces no duplicate read-model rows.

### 13.2 `Scriba.Target`

```elixir
@callback init(opts :: keyword()) :: {:ok, state :: term()}
@callback apply_batch(
            events :: [Scriba.Event.t()],
            handler_results :: [handler_result],
            projection :: %{name: String.t(), version: pos_integer()},
            stream_advances :: %{String.t() => non_neg_integer()},
            dead_letters :: [dead_letter],
            state :: term()
          ) :: {:ok, state} | {:error, reason :: term(), state}

# Optional. Lets a target declare which handler returns it can apply;
# anything it rejects is dead-lettered rather than reaching apply_batch/6.
@callback valid_result?(result :: handler_result) :: boolean()
```

`stream_advances` is one cursor per stream touched by the batch, not a
single position — the cursor is per stream (§8.1). `dead_letters` are
written in the same transaction as the read-model rows, which is what makes
"committed or dead-lettered, never neither" hold.

The target is responsible for the atomic write. The Ecto target
builds the Multi; the Test target appends to an Agent. The engine
treats them uniformly via this callback.

---

## 14. Release gate

Every release clears the same bar:

- The file layout matches §12.
- **Every document is audited against the code** — not only the ones the
  release touched. README.md, this file, MIGRATION.md, every `@moduledoc`
  and public `@doc` in `lib/`, `examples/bank/README.md`, `bench/README.md`
  and `docs/post-v0.1.md`. Each checkable claim — function names and
  arities, option names and defaults, return shapes, table and column
  names, telemetry events, test coverage, counts, measured numbers — needs
  a line of code that proves it. A document is never evidence for another
  document. Auditing only what changed is how a false claim survives a
  release: the documents drift against code they never mention.
- `mix compile --warnings-as-errors`, `mix credo --strict` and
  `mix dialyzer` are clean.
- `mix test.all` passes with `SCRIBA_TEST_DB_*` configured, so the
  real-Postgres suite (§10) actually runs rather than skipping.
- `mix docs` generates **no warnings**, and `mix hex.build` succeeds. This
  document names internal modules deliberately, and those carry
  `@moduledoc false`, so each mention would warn; `:skip_code_autolink_to`
  in `mix.exs` lists them instead. A new warning therefore means a genuinely
  broken reference, or a name that belongs on that list.
- **The CHANGELOG entry accounts for everything in the release.** Not that
  an entry exists — that `git log <previous tag>..HEAD` holds nothing a
  reader of the entry would be surprised by. An entry that understates its
  own release is the same drift as a stale document, and it is easy to
  produce: 0.1.4's entry listed a tooling fix and omitted the 39 corrected
  documentation claims that were the substance of the release. Behaviour
  changes, corrections, and anything a user would act on all belong in it;
  a refactor with no observable effect does not.
- The version tag exists before publishing — the package links point at
  `blob/v<version>/`, so publishing first yields 404s from HexDocs. Once a
  version is published the tag stays where it is: it names what was
  released, and a later correction rides to the next version rather than
  moving it.
- `examples/bank` runs end-to-end: a projection catching up to a
  Commanded event store with telemetry firing.

**The example app is not sufficient as an acceptance test.** It runs
Commanded's InMemory adapter, and InMemory diverges from a persistent
event store in subscription delivery and acknowledgement semantics — a
divergence that hid a defect making projections stall permanently against
`commanded_eventstore_adapter` while every test passed (fixed in 0.1.2).
Anything touching the source, the acknowledger or the commit path is also
exercised against a real event store via `bench/`.

---

## 15. Anti-goals (things to actively resist)

These are real failure modes for this kind of project. The following
are non-negotiable architectural commitments:

- A configuration DSL beyond keyword lists.
- A "plugin system" beyond the Source/Target behaviours.
- Any abstraction that requires reading more than two modules to
  understand a single event's flow from source to target.
- "Helpful" defaults that hide important failure modes (e.g. silent
  retry forever, swallowed exceptions, magic position recovery).
- Premature performance optimization. Correctness first; the
  benchmarks happen in v0.5.
- Compile-time projection registration. Projections are added at
  runtime via `start_projection/1`. The application supervisor knows
  nothing about user projections.

---

## 16. References

- Broadway: https://hexdocs.pm/broadway
- gen_statem: https://www.erlang.org/doc/man/gen_statem.html
- Commanded: https://hexdocs.pm/commanded
