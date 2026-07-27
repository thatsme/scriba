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
- Backpressure tuning knobs beyond Broadway defaults (v0.5)

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

`Scriba.info/1` returns a `Scriba.Info` struct with `:name`,
`:version`, `:status`, `:source`, `:target`, `:safe_position`,
`:stream_positions`. Lag and throughput are NOT in v0.1 — they live in
telemetry consumers.

`Scriba.list/0` returns `[%{name, version, state}]` for projections
currently registered in `Scriba.Registry`, including those in
`:stopped` state.

`Scriba.rebuild/2`, `Scriba.swap!/1`, `Scriba.reset/1` are stubs that
raise `Scriba.NotImplementedError` in v0.1. Document that explicitly.

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
  `:idle → :running → :paused → :draining → :stopped`. State-entry
  hooks via `enter` events keep pipeline start/stop logic clean.
  See §7.
- **No singleton `Scriba.PositionStore` GenServer.** Position lives in
  Postgres (authoritative) with an ETS cache per projection. See §8.
  The original design's PositionStore process is removed — it was a
  bottleneck.

### 6.2 Process registration

Coordinators register as `{:via, Registry, {Scriba.Registry, {:coordinator, name, version}}}`.
Broadway pipelines register as `{:via, Registry, {Scriba.Registry, {:pipeline, name, version}}}`.

### 6.3 Telemetry event surface (v0.1)

Four events fire from `Scriba.Projection.Pipeline`. Lag and throughput
events are explicitly v0.2 (see §2) — do not add them here without
amending that section.

| Event | Emitter | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:scriba, :projection, :event, :start]` | `handle_message/3` via `:telemetry.span/3` | `monotonic_time`, `system_time` | `projection`, `event_type`, `stream_id`, `position`, `telemetry_span_context` |
| `[:scriba, :projection, :event, :stop]` | same | `duration`, `monotonic_time` | same as `:start` |
| `[:scriba, :projection, :event, :exception]` | same | `duration`, `monotonic_time` | start metadata + `kind`, `reason`, `stacktrace` |
| `[:scriba, :projection, :batch, :stop]` | `handle_batch/4` (manual `:telemetry.execute/3`, success branch only) | `duration`, `batch_size` | `projection` |
| `[:scriba, :projection, :dead_letter]` | `handle_batch/4` (one per dead-lettered event, emitted AFTER Multi commit) | `system_time` | `projection`, `position`, `stream_id`, `event_type`, `error_kind` |
| `[:scriba, :projection, :started]` | `Scriba.Projection.Coordinator` (first `:initializing → :running`, fires once per Coordinator-process lifetime; Pipeline DOWN→re-running does NOT re-fire) | `system_time` | `projection` |
| `[:scriba, :projection, :paused]` | `Scriba.Projection.Coordinator` (on `:running → :paused`, after source pause signal sent) | `system_time` | `projection` |
| `[:scriba, :projection, :resumed]` | `Scriba.Projection.Coordinator` (on `:paused → :running`, after source resume signal sent) | `system_time` | `projection` |

Conventions:

- `projection` is `%{name: String.t(), version: pos_integer()}`.
- `event_type` is `event.type` (source-adapter-supplied; e.g. `"OrderPlaced"`).
- The exception event uses Erlang's span shape — `kind` / `reason` /
  `stacktrace` in **metadata**, not measurements. `:telemetry.span/3`
  re-raises after emission, so Broadway still observes the failure and
  marks the message. Phase C items 2 (retry) and 3 (dead-letter) will
  wrap **outside** this span to intercept before Broadway's default
  failure path.
- Source-side dedup (§9.x) returns `:skip` **without**
  emitting per-event telemetry — there was no handler call to measure.
  A separate `[:scriba, :projection, :event, :skipped]` event can be
  added if dedup visibility becomes operationally interesting.
- Batch `:stop` is emitted only on the success branch (Multi committed).
  Batch-failure observability is a hypothetical
  `[:scriba, :projection, :batch, :exception]` event, not implemented
  in v0.1. `batch_size` counts messages Broadway saw, including those
  whose handler returned `:skip` — they consumed pipeline capacity.

One additional event predates Phase C: `[:scriba, :projection, :cache_initialized]`
fires from `Scriba.Position.init_cache/3` with measurements
`%{wiped_count, preloaded_count}` and metadata `%{name, version, source}`
where `source` is `:postgres` or `:empty`.

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

### 7.2 Transitions

```
:initializing -- producer registered --> :running
:running      -- pause                --> :paused
:paused       -- resume               --> :running
:running      -- stop                 --> :draining --> :stopped
:paused       -- stop                 --> :stopped     (direct terminate)
:running      -- Pipeline DOWN        --> :initializing (rest_for_one respawn)
any           -- crash                --> (supervisor restarts to :initializing, then auto-:running)
```

`:running → :paused` is a **held-demand** transition (Phase C item 4),
not stop-and-restart. The Coordinator calls `Source.pause/1` on the
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
  parallelism: pos_integer(),
  pipeline_pid: pid() | nil,
  pipeline_ref: reference() | nil
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
  position bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (projection_name, projection_version)
);
```

The Ecto migration is provided by `Scriba.Migrations.up/0` and
`down/0`. Users invoke it from their own migration file.

### 8.2 Atomic update with read-model write

Every event commit looks like:

```elixir
Multi.new()
|> apply_handler_result(handler_return)            # user's intent
|> Multi.update_all(:scriba_position, position_query, set: [position: new_pos])
|> Repo.transaction()
```

If the transaction fails, neither the read-model write nor the
position update is applied. The event is retried (via Broadway
re-delivery) or routed to dead-letter after N failures.

### 8.3 ETS cache layer

One ETS table per projection, created when the Coordinator enters `:running`:

```elixir
:ets.new(table_name, [
  :set, :public, :named_table,
  {:write_concurrency, true},
  {:read_concurrency, true},
  {:decentralized_counters, true}
])
```

Workers update the cache after a successful commit. The cache is **not
authoritative** — it is a hot-read optimization for `Scriba.info/1`
and (in v0.2) the lag calculator. On Coordinator restart, the cache is
rebuilt from Postgres in the `:idle → :running` transition.

### 8.4 No PositionStore process

There is no GenServer mediating position reads or writes. Workers
write directly to Postgres (inside their Multi) and to the ETS cache.
Readers (info, telemetry) read from ETS first, fall back to Postgres
on miss.

---

## 9. Error handling and dead-letter

A handler can fail three ways:

1. Returns `{:error, reason}` — explicit
2. Raises — implicit, caught and converted to `{:error, exception}`
3. The Multi transaction fails — DB-level error

All three route to a dead-letter table:

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
  occurred_at timestamptz NOT NULL DEFAULT now()
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

## 10. Property tests (mandatory for v0.1)

Use `StreamData`. These three properties are non-negotiable.

### 10.1 P1 — Per-stream ordering

For any stream S, the sequence of events delivered to `handle/2` is a
prefix of the source order for S.

Generator: lists of events with monotonic per-stream sequence numbers,
multiple streams interleaved.
Assertion: handler-observed sequence numbers are monotonic per stream.

### 10.2 P2 — Position monotonicity

Across the whole projection, the committed position in Postgres is
monotonically non-decreasing across all observed states.

Generator: arbitrary event streams.
Assertion: `Scriba.info/1` position never goes backward across reads,
including across simulated worker crashes.

### 10.3 P3 — Effectively-once under crash

For each event, the handler's effect appears in the target exactly
once after recovery from arbitrary crash schedules.

Generator: `(events, crash_schedule)` pairs where `crash_schedule`
injects `Process.exit/2` at random points in the pipeline.
Test target: in-memory `Scriba.Target.Test` that records every
`{event_id, position}` it commits.
Assertion: deduped recorded list equals input event list.

If P3 fails, do not ship. Everything else is negotiable; this one is the
property that justifies Scriba's existence.

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

    # dev/test only
    {:commanded, "~> 1.4", optional: true},
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
- No metrics library. Telemetry events only. v0.2 adds `telemetry_metrics`.

---

## 12. File layout

```
scriba/
├── .formatter.exs
├── .gitignore
├── CHANGELOG.md
├── LICENSE                       # Apache-2.0
├── README.md
├── mix.exs
├── lib/
│   ├── scriba.ex                 # public API: start_projection, pause, resume, info, list
│   ├── scriba/
│   │   ├── application.ex
│   │   ├── supervisor.ex
│   │   ├── registry.ex
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
│   │   ├── partitioner.ex        # consistent hash logic
│   │   ├── telemetry.ex          # event-catalog moduledoc; no runtime code
│   │   ├── telemetry/
│   │   │   └── handler.ex        # default-handler slot (no-op in v0.1)
│   │   ├── migrations.ex         # up/0, down/0 for users to call
│   │   └── errors.ex             # Scriba.NotImplementedError, etc.
└── test/
    ├── test_helper.exs
    ├── support/
    │   ├── repo.ex
    │   ├── test_events.ex
    │   └── test_projection.ex
    ├── scriba/
    │   ├── projection_test.exs
    │   ├── coordinator_test.exs
    │   ├── partitioner_test.exs
    │   ├── source/
    │   │   └── commanded_test.exs
    │   └── target/
    │       └── ecto_test.exs
    └── property/
        ├── ordering_test.exs       # P1
        ├── monotonicity_test.exs   # P2
        └── crash_recovery_test.exs # P3
```

Do not add files outside this layout without justification. Do not
create empty placeholder files for future versions.

---

## 13. Behaviours

### 13.1 `Scriba.Source`

```elixir
@callback child_spec(opts :: keyword()) :: Supervisor.child_spec()
@callback start_link(opts :: keyword()) :: GenServer.on_start()

# Broadway producer interface — Source modules implement
# Broadway.Producer behaviour and yield events as
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

* `Scriba.Source.Commanded` calls `subscribe_to(_, _, name, _, :origin)`
  with a stable subscription `name`. Commanded's event store persists
  that subscription's acked position; on Pipeline restart, the
  re-subscription resumes from the persisted position.
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
by killing the Pipeline child (which kills the source) and observing
that the new source's replay-from-zero produces no duplicate read-model
rows.

### 13.2 `Scriba.Target`

```elixir
@callback init(opts :: keyword()) :: {:ok, state :: term()}
@callback apply_batch(
            events :: [Scriba.Event.t()],
            handler_results :: [term()],
            projection :: %{name: String.t(), version: pos_integer()},
            new_position :: non_neg_integer(),
            state :: term()
          ) :: {:ok, state} | {:error, reason :: term(), state}
```

The target is responsible for the atomic write. The Ecto target
builds the Multi; the Test target appends to an Agent. The engine
treats them uniformly via this callback.

---

## 14. What "done" looks like for v0.1

Checklist for shipping `0.1.0`:

- [ ] All files in §12 exist and are non-empty
- [ ] `mix compile --warnings-as-errors` is clean
- [ ] `mix credo --strict` passes
- [ ] `mix dialyzer` passes
- [ ] `mix test` passes including the three property tests in §10
- [ ] `mix docs` generates without warnings
- [ ] README has: install, 5-line example, link to design doc, scope
      statement (what Scriba is/is not from §1)
- [ ] CHANGELOG has a `0.1.0` entry
- [ ] LICENSE is Apache-2.0
- [ ] `mix hex.build` succeeds
- [ ] A working example app in `examples/` consumes a real Commanded
      event store and runs a real projection end-to-end

The example app is the acceptance test. If `mix run` on the example
does not show a projection catching up to a Commanded event store
with telemetry firing, v0.1 is not done.

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
