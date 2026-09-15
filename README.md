# Scriba

A projection engine for Elixir event-sourced systems. Pairs with
Commanded for the event-sourcing side; replaces the role
[`commanded_ecto_projections`](https://github.com/commanded/commanded-ecto-projections)
used to play before it stopped being actively maintained.

```elixir
defmodule MyApp.Projections.Orders do
  use Scriba.Projection,
    name: "orders",
    source: {Scriba.Source.Commanded, application: MyApp.CommandedApp},
    target: {Scriba.Target.Ecto, repo: MyApp.Repo},
    parallelism: 16

  def handle(%OrderPlaced{} = event, _meta) do
    {:insert, %OrderReadModel{id: event.order_id, status: "pending"}}
  end

  def handle(%OrderShipped{order_id: id}, _meta) do
    {:update, OrderReadModel, [id: id], set: [status: "shipped"]}
  end

  def handle(_, _), do: :skip
end

{:ok, _} = Scriba.start_projection(MyApp.Projections.Orders)
```

That's the API. The rest is operational scaffolding you get for free.

**Already running `commanded_ecto_projections`?** Read
[`MIGRATION.md`](MIGRATION.md). Short version: your existing cursor
carries over — both libraries track Commanded's global `event_number`,
so you hand your `last_seen_event_number` to Scriba as `:start_from` and
cut over in place. No read-model rebuild, no maintenance window.

---

## Status

**v0.1.** The architectural contract is frozen
([`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md)) and the operational
primitives — telemetry, dead-letter routing, retry policy, real
pause/resume — are in place. Property tests cover per-stream ordering,
position monotonicity, and effectively-once delivery under crash.

What's deliberately out of scope for v0.1:

- Lag and throughput metrics beyond raw telemetry events (v0.2 +
  LiveView dashboard).
- Online rebuild / shadow targets / atomic swap (v0.3).
- Sources other than Commanded; targets other than Ecto/Postgres
  (v0.4).
- Multi-target fan-out (v0.4).

See [`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md) §2 for the full
in-scope / out-of-scope split.

---

## Why this exists

You're running Commanded. You need read models. The library you'd
have reached for —
[`commanded_ecto_projections`](https://github.com/commanded/commanded-ecto-projections)
— has been unmaintained for years and has known sharp edges around
crash recovery, per-stream ordering across parallelism, and lag
visibility.

Scriba is an opinionated rewrite of that role with three principles:

1. **Correctness over throughput.** Position update and read-model
   write commit atomically inside a single `Ecto.Multi`. There is no
   configuration that lets you turn this off, because you should not
   want to.
2. **Per-stream ordering is preserved.** All events for a given
   `stream_id` route to the same processor via consistent hash. Within
   a processor, events are serial. Across streams, events are
   parallel.
3. **Sharp edges are documented, not hidden.** Dead-lettered events
   advance the cursor (skip-and-continue, not block-the-projection).
   The handler return contract is six tagged tuples, not a DSL. Lag is
   a telemetry-consumer concern, not an engine feature.

Migrating an existing projector is a mechanical rewrite —
`project %Event{}, fn multi -> ... end` becomes `def handle(%Event{}, meta)`
returning a tagged tuple. [`MIGRATION.md`](MIGRATION.md) covers the
rewrite, the `Ecto.Multi` differences, the callbacks with no equivalent
(`after_update/3`, `schema_prefix/1`, `consistency: :strong`), and cursor
carry-over.

---

## Installation

```elixir
defp deps do
  [
    {:scriba, "~> 0.1"},

    # Optional — needed only if you use Scriba.Source.Commanded,
    # which is the only source shipped in v0.1.
    {:commanded, "~> 1.4"}
  ]
end
```

`:commanded` is Scriba's one optional dependency: nothing in the engine
references it statically, so Scriba compiles without it, and
`Scriba.Source.Commanded.start_link/1` raises with instructions if you
configure the Commanded source without adding it.

Everything else arrives transitively and is **not** optional —
`:ecto_sql` and `:postgrex` back the position cursor and dead-letter
tables (`Scriba.Position`, `Scriba.DeadLetter`, `Scriba.Migrations`), not
merely `Scriba.Target.Ecto`; `:broadway` is the pipeline runtime;
`:telemetry` and `:jason` are used throughout. You do not list them
yourself, but they will be in your dependency tree.

### Scriba is a Broadway topology

Worth knowing before you read the failure-modes section below, which is
written in Broadway's vocabulary: each projection is a
[Broadway](https://hexdocs.pm/broadway) pipeline. The source is a Broadway
producer, `:parallelism` sets the processor concurrency, `:batch_size` /
`:batch_timeout` configure the batcher, and event acknowledgement runs
through a Broadway acknowledger that Scriba implements against your event
store. You never write Broadway code — but when this README says "the
batch is marked failed", that is `Broadway.Message.failed/2`, and Broadway
is where the retry-on-redelivery behaviour comes from.

Add a migration to your repo for Scriba's tables:

```elixir
defmodule MyApp.Repo.Migrations.AddScribaTables do
  use Ecto.Migration
  def up,   do: Scriba.Migrations.up()
  def down, do: Scriba.Migrations.down()
end
```

This creates two tables in your read-model database:

- `scriba_positions` — per-stream cursor (one row per `{projection,
  version, stream_id}`).
- `scriba_dead_letters` — failed events for inspection / manual replay.

Start projections during your application boot. The idiomatic pattern
is a small `Task` in your supervision tree that calls
`Scriba.start_projection/1` once Ecto and Commanded are up:

```elixir
# Bank.Application or equivalent
children = [
  MyApp.Repo,
  MyApp.CommandedApp,
  MyApp.Projections.Starter   # Task that calls Scriba.start_projection
]
```

See [`examples/bank/lib/bank/projections/starter.ex`](https://github.com/thatsme/scriba/blob/main/examples/bank/lib/bank/projections/starter.ex)
for the working pattern.

---

## Core concepts

### `name` and `version`

A projection's identity is `(name, version)`. `name` is the stable
logical label ("orders"). `version` is an integer (default `1`) you
bump when you want to run a *new* projection side-by-side with the old
one during a cutover.

```elixir
# orders v1 — the current production projection
defmodule MyApp.Projections.OrdersV1 do
  use Scriba.Projection, name: "orders", version: 1, ...
end

# orders v2 — running alongside, populating a new read model
defmodule MyApp.Projections.OrdersV2 do
  use Scriba.Projection, name: "orders", version: 2, ...
end
```

**Do not** encode the version in the name (`name: "orders_v2"`). The
macro warns at compile time when it sees that pattern, because
`commanded_ecto_projections` historically used it and it makes
side-by-side versioning awkward. See architecture §5.

### Handler return shapes

Your `handle(event, meta)` clauses must return one of these six values:

| Return | Effect |
|---|---|
| `:skip` | Event acknowledged, no read-model write. Cursor still advances. |
| `{:insert, schema_struct}` | `Ecto.Multi.insert/3` |
| `{:update, schema_module, filter_keyword, [set: keyword]}` | `Ecto.Multi.update_all/4` filtered by the keyword |
| `{:delete, schema_module, filter_keyword}` | `Ecto.Multi.delete_all/3` |
| `{:multi, %Ecto.Multi{}}` | Merged into the batch's Multi — escape hatch for `:inc`, complex queries, etc. |
| `{:error, reason}` | Routes to dead-letter (after retries, if enabled). |

Raising an exception is also valid; it's converted to a dead-letter
entry whose `error_kind` is the exception's module name. Per
architecture §4.2.

### The `meta` map

The second argument to `handle/2`:

```elixir
%{
  id:          "event-uuid",
  stream_id:   "aggregate-uuid",
  type:        "OrderPlaced",
  position:    42,
  metadata:    %{correlation_id: "..."},
  occurred_at: ~U[2026-05-14 12:00:00Z]
}
```

Use `:id` for idempotency keys (globally unique). Use `:position` for
ordering within Scriba's internal accounting; don't use it as a
stable identifier across event-store rebuilds.

### Per-stream ordering

Events with the same `stream_id` route to the same processor via
consistent hashing — different events on the same stream are *never*
processed concurrently. Events across streams *are* parallel,
bounded by `:parallelism`.

This is the invariant that makes "balance += amount" projections
correct without explicit locking.

### Atomic position commit

Every successful batch commits the user's read-model writes AND the
per-stream cursor advances in **one** `Ecto.Multi` transaction. There
is no observable state where the read model advanced but the cursor
didn't — or vice versa.

This is the property that makes crash recovery work: on Coordinator
restart, the source is told to resume from the durably committed
cursor, and Scriba's source-side dedup skips any events the upstream
re-delivers below that cursor.

---

## Operational features

### Telemetry

Twelve events fire — from the Pipeline, the Coordinator, position-cache init,
and the source. The full surface table is in `Scriba.Telemetry`'s moduledoc
and in architecture §6.3. Highlights:

```
[:scriba, :projection, :event, :start | :stop | :exception]
[:scriba, :projection, :event, :skipped]
[:scriba, :projection, :batch, :stop]
[:scriba, :projection, :dead_letter]
[:scriba, :projection, :started | :paused | :resumed]
[:scriba, :projection, :cache_initialized]
[:scriba, :projection, :halted]
[:scriba, :source, :batch, :failed]
```

`:event :stop` fires once per successful handler invocation —
counting these gives you exact "events processed" without doing
position-arithmetic across streams. `:dead_letter` fires once per
dead-lettered event with `{name, version, position, stream_id,
event_type, error_kind}` metadata for alerting.

`[:scriba, :source, :batch, :failed]` means a batch did not commit, nothing
in it was acknowledged, and the source is restarting to replay from its last
durable checkpoint. Isolated occurrences are normal under transient database
trouble; a sustained stream of them means no progress.

`[:scriba, :projection, :halted]` is the page. The projection hit a
structural failure — a column that doesn't exist, a missing privilege — and
stopped on purpose, because replaying it would loop forever and
dead-lettering it would destroy a batch over a fixable deploy-ordering
mistake. Nothing is lost; nothing proceeds either. The metadata carries the
SQLSTATE.

Scriba attaches no handlers of its own — it emits and gets out of the
way, so it never competes with your observability stack. You write your
own with `:telemetry.attach_many/4` in your application's `start/2`.

### Dead-letter routing

A failing event — handler returned `{:error, _}` or raised, AFTER
retry exhaustion — gets a row inserted into `scriba_dead_letters`
**atomically with the cursor advance**. The projection does not
block on bad events. This is a deliberate sharp edge, documented at
architecture §9.2:

> "When an event is dead-lettered, the position advances past it.
> The alternative — blocking the projection until the bad event is
> resolved — is the #2 complaint on ElixirForum after lag visibility."

If you want block-on-failure semantics for a specific projection,
you build that on top: subscribe to `[:scriba, :projection,
:dead_letter]` telemetry, page someone, manually replay from the
dead-letter table once they've fixed the underlying issue.

### Retry policy

Default: 3 attempts with exponential backoff (100ms, 1s, 10s) before
dead-letter. Configurable per projection:

```elixir
use Scriba.Projection,
  ...
  retry: [max_attempts: 5, backoff: [100, 500, 2000, 10_000]]
```

Or `retry: false` to opt out (one attempt, immediate dead-letter on
failure).

The retry loop is in-handler `Process.sleep` — see architecture §9.1
and §9 for why this is the right primitive rather
than `Process.send_after` (Broadway's processor model). Each retry
attempt re-invokes `:telemetry.span/3`, so per-attempt
`:event :start` / `:event :stop` / `:event :exception` events fire.
Operators counting `:event :start` per `event_id` can see retry
activity without a dedicated `:retry` event.

### Pause / resume

`Scriba.pause(MyApp.Projections.Orders)` signals the source to stop
yielding new events. The Pipeline tree stays alive; in-flight events
finish their commit lifecycle. `Scriba.resume(...)` reverses the
signal. Both fire telemetry; both are honest about asynchrony (the
source-pause signal is `send/2`, so by the time `pause/1` returns the
signal is in the source's mailbox but the source's `handle_info`
may not yet have run).

`pause` on `:paused` and `resume` on `:running` return `{:error,
{:invalid_state, _}}` — they are deliberately **not idempotent**.
Silent idempotency hides bugs. If you want "make sure this is
paused" semantics, check `Scriba.info/1` first or pattern-match the
matching-state error case as success.

---

## Failure modes worth knowing

### Handler raises

Re-raise is caught by Scriba, the exception is logged as a
`:event :exception` telemetry event, and the event enters the retry
loop. After retry exhaustion, the original exception's struct and
stacktrace are recorded in `scriba_dead_letters` with `error_kind`
equal to the exception module name (e.g. `"Elixir.ArgumentError"`).

### Multi transaction fails

What happens depends on *why*, and Scriba reads that from the SQLSTATE
rather than guessing (`Scriba.Failure`). Guessing from "did some events
succeed?" is wrong in both directions: resource pressure fails
non-uniformly, so a partial success looks deterministic when it isn't;
and handler code deployed ahead of its migration fails uniformly, so it
looks transient when it very much isn't.

**Transient** (connection loss, deadlock, serialization failure, resource
exhaustion, cancelled query). Nothing in the batch is acknowledged —
including the events that succeeded, because event-store acks are
prefix-acks and cannot express a gap. The source stops its producer, the
subscription rewinds to its last durable checkpoint, and the batch is
redelivered. Source-side dedup filters whatever did commit.

**Integrity** (unique violation, NOT NULL, foreign key, check constraint,
numeric overflow). Deterministic and specific to one event, so replaying
it forever is pointless. The batch is re-applied one transaction per
event: the offending event lands in `scriba_dead_letters` with
`error_kind` `"commit:23505 (unique_violation)"` or similar, the rest
commit, and the projection keeps moving.

**Structural** (undefined column or table, insufficient privilege, and
anything Scriba cannot classify). Neither response is safe —
dead-lettering would destroy a batch over a fixable deploy-ordering
mistake, replaying would loop forever — so the projection **halts** and
says so via `[:scriba, :projection, :halted]` and a log line naming the
SQLSTATE. Nothing is acknowledged and no cursor moves, so nothing is
lost. It resumes when you fix the cause and restart.

**Multi failures do not retry through the per-event retry policy.**
The retry layer wraps the handler call, not the Multi commit. If
your DB is intermittently failing, you want it to recover at the DB
level — not for individual events to retry-then-dead-letter against
a sick database.

Per-stream ordering survives all three: in the per-event pass a stream
stops at its first unresolved event rather than skipping past it.

> **Verification status.** The replay path — the source refusing to
> acknowledge, killing its producer, rewinding the subscription, backing off
> and redelivering — is covered by **unit tests only**. It has never been
> exercised against a real event store, and conservation across a database
> outage (`events delivered == read-model rows + dead letters + skipped`) is
> unverified. The reason is that Scriba's test double, `Scriba.Test.Source`,
> discards failed messages instead of redelivering them, so the suite cannot
> replay anything. The transient/integrity/structural classification above
> *is* verified against real Postgres, including that a `docker stop` emits
> `57P01` and is treated as transient.


### Source redelivers events the projection has already committed

This happens after Pipeline restart — Commanded's subscription
resumes from its acked position, which may be behind Scriba's
durably committed position. Source-side dedup at the Pipeline's
`handle_message/3` (architecture §3.4) catches these: events whose
position is at or below the committed cursor for their stream are
returned as `:skip` without invoking the handler.

This is the property `P3 — effectively-once under crash` exists to
verify. Its real-Postgres property test is
`test/property_db/pd3_cursor_resume_test.exs` (PD3 — recovery resumes
from the committed cursor, 100 iterations against real Postgres), with
the integration-test side in `test/scriba/projection/pipeline_test.exs`.

---

## Example app

[`examples/bank/`](https://github.com/thatsme/scriba/tree/main/examples/bank)
is a self-contained Mix project
that demonstrates the full path: real Commanded
(`Commanded.EventStore.Adapters.InMemory` for fast iteration), real
Ecto, real read model. The projection module itself is ~50 lines
including comments.

```sh
cd examples/bank
mix deps.get
mix bank.setup   # creates the database, runs migrations
mix bank.demo    # opens 3 accounts, dispatches 50 random ops, prints balances
```

The demo's wait-for-completion uses a telemetry counter on
`:event :stop` — exactly the pattern you'd reach for in your own
test code. See
[`examples/bank/lib/mix/tasks/bank.demo.ex`](https://github.com/thatsme/scriba/blob/main/examples/bank/lib/mix/tasks/bank.demo.ex).

The example app is **not** included in the Hex package tarball — these
links go to GitHub. Clone the repo to run it.

---

## Running Scriba's own tests

| Command | What runs | Requires |
|---|---|---|
| `mix test.fast` | Non-property tests | — |
| `mix test.property_db` | Real-Postgres property tests in `test/property_db/` | `SCRIBA_TEST_DB_*` env vars |
| `mix test.all` | Everything ExUnit will run with the current environment | — for always-runnable parts; env vars for `property_db` |

`mix test.all` is the canonical full-suite command. Status reports
should name the command: "fast suite: 99/0" rather than "99 tests, 0
failures." Naming the command in reports avoids the ambiguity that
bit us during the diagnostic phase.

### Real-Postgres property tests

Property tests in `test/property_db/` (Phase B of the rebuild) need a
live Postgres. Set all five of:

| Variable | Example |
|---|---|
| `SCRIBA_TEST_DB_HOST` | `localhost` |
| `SCRIBA_TEST_DB_PORT` | `5433` |
| `SCRIBA_TEST_DB_NAME` | `scriba_test` |
| `SCRIBA_TEST_DB_USER` | `postgres` |
| `SCRIBA_TEST_DB_PASS` | `postgres` |

Local convenience: copy `.env.local.example` to `.env.local` and fill
in real values. `config/test.exs` auto-loads it; `.env.local` is
gitignored. Shell environment variables override `.env.local`.

**Policy is all-or-nothing-or-error:** all five set → tests run; all
five unset → tests excluded with a startup message; any subset
partially set → `config/test.exs` raises at config load. Partial
configuration is treated as a misconfiguration, not a graceful
degrade.

The database must already exist; `mix test` does not create it.
Migrations run in `test_helper.exs` against an existing connection.
`docker compose up -d` starts a Postgres 16 on port 5433 with
`scriba_test` already created, matching the example values above.

---

## Documentation

- [`MIGRATION.md`](MIGRATION.md) — migrating from
  `commanded_ecto_projections`: `project/2` → `handle/2`, `Ecto.Multi`
  differences, and how to carry your existing cursor across so you
  cut over in place instead of rebuilding read models.
- [`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md) — the
  architectural contract. Read this before opening a PR that
  changes engine behavior.
- [`examples/bank/README.md`](https://github.com/thatsme/scriba/blob/main/examples/bank/README.md)
  — example app walkthrough.
- `Scriba.Telemetry` moduledoc — full v0.1 telemetry event surface.

---

## Versioning

Scriba follows semver. The v0.1.0 API surface (the seven functions
in `Scriba` plus the macro at `Scriba.Projection`) is frozen — no
breaking changes within 0.1.x. New optional features may land in
0.1.x patch releases.

`Scriba.Target` and `Scriba.Source` behaviours are not yet frozen
— v0.4 will widen them when non-Commanded sources and non-Ecto
targets land. Custom adapter authors should pin against a specific
0.1.x.

---

## License

Apache-2.0. See [LICENSE](https://github.com/thatsme/scriba/blob/main/LICENSE).

---

## Contributing

Issues and PRs welcome at https://github.com/thatsme/scriba.
Architecture-affecting changes should reference the relevant section
of `SCRIBA_ARCHITECTURE.md`; deliberate departures from the contract
require documentation in the PR explaining why.
