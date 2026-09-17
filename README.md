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
so you hand your `last_seen_event_number` to the source as `:start_from`
(`source: {Scriba.Source.Commanded, application: MyApp, start_from: 12_345}`)
and cut over in place. No read-model rebuild, no maintenance window.

---

## Status

**v0.2.** The architectural contract is frozen
([`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md)) and the operational
primitives — telemetry, dead-letter routing and inspection, retry policy,
real pause/resume, lag reporting, multi-node standby — are in place.
Property tests cover per-stream ordering, cursor/read-model consistency,
and resume-from-cursor after a restart; fault injection against real
Postgres covers the failure taxonomy; a separate harness (`bench/`)
exercises acknowledgement, standby takeover and the watermark against a
real event store. Effectively-once under injected crash schedules is
enforced structurally rather than by a property test — see architecture
§10.

What's deliberately out of scope:

- A LiveView dashboard. `broadway_dashboard` already renders Scriba
  projections — see "Seeing a projection in LiveDashboard" below.
- Throughput metrics of Scriba's own — Broadway's batch telemetry and the
  per-event `:stop` events already carry the rate.
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
— last saw a release in January 2024 and has known sharp edges around
crash recovery, per-stream ordering across parallelism, and lag
visibility. The alternative that is maintained, `Commanded.Event.Handler`,
is compared against Scriba in the next section.

Scriba is an opinionated rewrite of that role with three principles:

1. **Correctness over throughput.** Position update and read-model
   write commit atomically inside a single `Ecto.Multi`. There is no
   configuration that lets you turn this off, because you should not
   want to.
2. **Per-stream ordering is preserved.** All events for a given
   `stream_id` route to the same processor via modular hashing of the
   stream id. Within a processor, events are serial. Across streams,
   events are parallel.
3. **Sharp edges are documented, not hidden.** Dead-lettered events
   advance the cursor (skip-and-continue, not block-the-projection).
   The handler return contract is six return shapes, not a DSL. Lag is
   measured in time, from the watermarked event's own timestamp, because
   Commanded's event store adapter behaviour exposes no head position to
   count events against.

Migrating an existing projector is a mechanical rewrite —
`project %Event{}, fn multi -> ... end` becomes `def handle(%Event{}, meta)`
returning a tagged tuple. [`MIGRATION.md`](MIGRATION.md) covers the
rewrite, the `Ecto.Multi` differences, the callbacks with no equivalent
(`after_update/3`, `schema_prefix/1`, `consistency: :strong`), and cursor
carry-over.

---

## Do you need Scriba?

Commanded already ships `Commanded.Event.Handler`, and for many projections
it is the right answer. It is worth being specific about where the line is.

Everything below was checked against **Commanded 1.4.11** (July 2026), which
is what Scriba's own test suite runs against. If you are reading this against
a later release, check its changelog — this section is a snapshot, and saying
so is more useful than pretending otherwise.

### What `Commanded.Event.Handler` gives you

`:concurrency` starts several handler processes, and a `partition_by/2`
callback routes events so that related ones land on the same process and
stay ordered. `handle_batch/1` with `:batch_size` delivers events in
batches, and since 1.4.10 `:batch_timeout` flushes a partial batch on time
as well as on size — so a low-volume stream no longer waits for a batch to
fill. `error/3` is called when a handler fails and you decide what happens —
`{:retry, context}`, `{:retry, delay, context}`, `:skip` or
`{:stop, reason}`. With no `error/3` at all, the handler stops on the reason
your handler returned.

That covers a great many projections, costs no extra dependency, and is
maintained by the people who maintain your event store.

Two constraints are worth knowing before you compare. Batching and
concurrency cannot be combined — setting both raises — so you choose between
parallel handlers and batched writes. And a handler acknowledges an event
once `handle/2` returns, so "what has this projection applied?" is answered
by the subscription's checkpoint, not by anything in your read-model
database.

### What Scriba adds

- **The cursor and the read-model write commit together.** One
  `Ecto.Multi`, one transaction. A crash cannot leave the read model ahead
  of the cursor or behind it, and there is no configuration to turn that
  off.
- **Batching and per-stream parallelism at the same time.** Events for one
  `stream_id` are serial; different streams run in parallel; commits are
  batched underneath both.
- **A failure taxonomy rather than a callback.** `Scriba.Failure` reads the
  SQLSTATE and picks the response that terminates: transient errors replay,
  integrity errors dead-letter that one event and continue, structural
  errors halt the projection loudly. You are not asked to classify database
  failures in application code.
- **A dead-letter table you can query** — `Scriba.dead_letters/2` and
  `dead_letter_stats/2`, with the error-kind distribution that separates a
  poison event from a schema problem.
- **An answer to "how far behind is it?"** — a contiguous watermark,
  `:lag_ms` on `Scriba.info/1`, and `[:scriba, :projection, :lag]` every
  `:lag_interval` milliseconds (default `5_000`; `0` disables it), so an
  idle projection still reports.
- **Rebuilds as a procedure** — `(name, version)` runs a new version beside
  the old one against the same events, and `Scriba.reset/2` clears a
  version's cursors and watermark so it can run again from the start. It
  does not touch your read model — Scriba does not know which tables your
  handler writes (`REBUILDING.md`).
- **Standby on every other node** — one node holds the subscription, the
  rest wait and take over.
- **Tests without a pipeline** — `Scriba.Testing.project/3` runs your
  handlers and commits through the real target so you assert on rows.

### When the handler is the better choice

- The projection is small, the volume is low, and a missed event is
  something you would notice and fix by hand.
- Your handler is naturally idempotent, so at-least-once delivery costs you
  nothing.
- You do not want a third table, another dependency, or another thing to
  upgrade.
- You need something Scriba deliberately does not do — non-Postgres targets,
  fan-out to several read models, custom partitioning.

### The honest summary

Scriba is for projections where being wrong is expensive and being down is
noticeable: where you want a crash to be recoverable rather than
investigated, a bad event to be quarantined rather than blocking, and "is it
caught up?" to have a numeric answer. If none of that is pressing, the
handler you already have is less machinery for the same result.

Migrating later is not costly either way — a Scriba projection is a module
with `handle/2` clauses, which is very close to what you already have.

---

## Installation

```elixir
defp deps do
  [
    {:scriba, "~> 0.2"},

    # Optional — needed only if you use Scriba.Source.Commanded,
    # which is the only source that ships today.
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
`:telemetry` is used throughout, and `:jason` indirectly, by Postgrex, to
encode the dead-letter table's `event_data` column. You do not list them
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

### Catch-up throughput depends on `:buffer_size`

The event store decides how many events it will send before it requires an
acknowledgement, and its default is one. Scriba acknowledges after the batch
commits, so with a single event in flight the batcher waits out its whole
`:batch_timeout` before releasing the next one — which bounds catch-up far
below anything `:parallelism` can affect. Measured against a real EventStore,
5,000 events over 100 streams: **9.1 events/sec** at the adapter default,
**6,002 events/sec** with `buffer_size: 500`.

```elixir
source: {Scriba.Source.Commanded,
         application: MyApp.CommandedApp,
         buffer_size: 500}
```

Scriba sets no default of its own. Raising the buffer trades memory and
redelivered-work-after-a-crash for throughput — see
`Scriba.Source.Commanded` for the full table and the tradeoff.

Add a migration to your repo for Scriba's tables:

```elixir
defmodule MyApp.Repo.Migrations.AddScribaTables do
  use Ecto.Migration
  def up,   do: Scriba.Migrations.up()
  def down, do: Scriba.Migrations.down()
end
```

This creates three tables in your read-model database:

- `scriba_positions` — per-stream cursor (one row per `{projection,
  version, stream_id}`).
- `scriba_dead_letters` — failed events for inspection / manual replay.
- `scriba_watermarks` — the contiguous global position per projection.

Upgrading from Scriba 0.1.x, whose schema had the first two, means a second
migration that names the version you already have:

```elixir
def up,   do: Scriba.Migrations.up(from: 1)
def down, do: Scriba.Migrations.down(to: 1)
```

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
| `:skip` | Event acknowledged, no read-model write, **cursor does not advance** for that stream. |
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

Events with the same `stream_id` route to the same processor —
`:erlang.phash2(stream_id, parallelism)`, plain modular hashing rather
than a consistent-hash ring — so different events on the same stream are
*never* processed concurrently. Events across streams *are* parallel,
bounded by `:parallelism`.

This is the invariant that makes "balance += amount" projections
correct without explicit locking.

### Atomic position commit

Every successful batch commits the user's read-model writes AND the
per-stream cursor advances in **one** `Ecto.Multi` transaction. There
is no observable state where the read model advanced but the cursor
didn't — or vice versa.

This is the property that makes crash recovery work. On restart the source
resumes from the event store's own subscription checkpoint, which may sit
behind Scriba's durably committed cursor; Scriba's dedup then skips any
event the upstream re-delivers at or below that cursor. Scriba never hands
the cursor back to the source — `:start_from` is read once, from your
config, when the source starts.

---

## Operational features

### Telemetry

Fifteen events fire — from the Pipeline, the Coordinator, position-cache
init, and the source. The full surface table is in `Scriba.Telemetry`'s moduledoc
and in architecture §6.3. Highlights:

```
[:scriba, :projection, :event, :start | :stop | :exception]
[:scriba, :projection, :event, :skipped]
[:scriba, :projection, :batch, :stop]
[:scriba, :projection, :dead_letter]
[:scriba, :projection, :started | :paused | :resumed]
[:scriba, :projection, :cache_initialized]
[:scriba, :projection, :lag]
[:scriba, :source, :standby | :subscribed]
[:scriba, :projection, :halted]
[:scriba, :source, :batch, :failed]
```

`:event :stop` fires once per successful handler invocation —
counting these gives you exact "events processed" without doing
position-arithmetic across streams. `:dead_letter` fires once per
dead-lettered event, with `projection: %{name, version}` plus `position`,
`stream_id`, `event_type` and `error_kind` metadata for alerting.

`[:scriba, :source, :batch, :failed]` means a batch did not commit and
nothing in it was acknowledged. For a transient failure the source then
restarts to replay from its last durable checkpoint; for a structural one it
deliberately stays stopped, because no replay can fix a missing column.
Isolated occurrences are normal under transient database trouble; a sustained
stream of them means no progress.

`[:scriba, :projection, :halted]` is the page. Either the projection hit a
structural failure — a column that doesn't exist, a missing privilege — or
every attempted write in a batch failed on integrity grounds, which is a
schema the handler no longer matches. Both stop on purpose, because
replaying would loop forever and dead-lettering would destroy a batch over a
fixable deploy-ordering mistake. Nothing is lost; nothing proceeds either.
The `failure` metadata carries a SQLSTATE label when the cause is a
`Postgrex.Error`, and otherwise names what it was — a constraint, or
`{:integrity_wipeout, n}`.

Scriba attaches no handlers of its own — it emits and gets out of the
way, so it never competes with your observability stack. You write your
own with `:telemetry.attach_many/4` in your application's `start/2`.

### Dead-letter routing

A failing event — handler returned `{:error, _}` or raised, AFTER
retry exhaustion — gets a row inserted into `scriba_dead_letters`
**atomically with the cursor advance**. The projection does not
block on bad events. This is a deliberate sharp edge, documented at
architecture §9.2:

> "When an event is dead-lettered, the position advances past it. The
> alternative — blocking the projection until the bad event is resolved —
> stops every subsequent event for one bad one, and does it silently."

If you want block-on-failure semantics for a specific projection,
you build that on top: subscribe to `[:scriba, :projection,
:dead_letter]` telemetry, page someone, manually replay from the
dead-letter table once they've fixed the underlying issue.

### Retry policy

Default: 3 attempts before dead-letter, sleeping 100ms then 1s between them.
The third backoff entry (10s) only comes into play if you raise
`max_attempts` to 4. Configurable per projection:

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
Operators counting `:event :start` per `{stream_id, position}` — the span
metadata carries no event id — can see retry activity without a dedicated
`:retry` event.

### Pause / resume

`Scriba.pause(MyApp.Projections.Orders)` signals the source to stop
yielding new events. The Pipeline tree stays alive; in-flight events
finish their commit lifecycle. `Scriba.resume(...)` reverses the
signal. Both fire telemetry; both are honest about asynchrony (the
source-pause signal is `send/2`, so by the time `pause/1` returns the
signal is in the source's mailbox but the source's `handle_info`
may not yet have run).

A pause stops the source yielding; it does not stop the event store pushing.
`Scriba.Source.Commanded`'s subscription keeps filling the producer's pending
queue for the duration, so memory grows with whatever the upstream produces
in the pause window. Pause to let a migration land, not as an off switch.

A pause survives a Pipeline restart — the Coordinator reapplies it to the
replacement producer, which starts unpaused — but not a restart of the
projection itself, which comes back `:running`.

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
SQLSTATE. Nothing in the halting batch is acknowledged, so nothing is lost;
work the per-event pass had already committed before it reached the
offending event stands, and dedup filters it on redelivery. It resumes when
you fix the cause and restart the projection.

**Multi failures do not retry through the per-event retry policy.**
The retry layer wraps the handler call, not the Multi commit. If
your DB is intermittently failing, you want it to recover at the DB
level — not for individual events to retry-then-dead-letter against
a sick database.

Per-stream ordering survives all three: in the per-event pass a stream
stops at its first unresolved event rather than skipping past it.

> **Verification status.** The replay path — the source refusing to
> acknowledge, killing its producer, rewinding the subscription, backing off
> and redelivering — is exercised by the suite through the `Scriba.Test.Source`
> double, which requeues failed messages in position order, but **not against a
> real event store**: conservation across a database outage
> (`events delivered == read-model rows + dead letters + skipped`) is
> unverified there. Of the failure classification, `:integrity` and
> `:structural` are verified against real Postgres
> (`test/property_db/e3_fault_injection_test.exs`); `:transient` is covered by
> unit tests against constructed `Postgrex.Error` structs, including the
> `57P01` a `docker stop` emits.


### Source redelivers events the projection has already committed

This happens after Pipeline restart — Commanded's subscription
resumes from its acked position, which may be behind Scriba's
durably committed position. Source-side dedup at the Pipeline's
`handle_message/3` (architecture §3.4) catches these: events whose
position is at or below the committed cursor for their stream are
returned as `:skip` without invoking the handler.

`test/property_db/pd3_cursor_resume_test.exs` verifies this against real
Postgres over 100 iterations: after a clean stop, a projection restarted
with `:start_from` set to the committed cursor processes nothing it has
already applied. The integration-test side, where the source replays from
zero and dedup absorbs it, is in
`test/scriba/projection/pipeline_test.exs`.

---

## Example app

[`examples/bank/`](https://github.com/thatsme/scriba/tree/main/examples/bank)
is a self-contained Mix project
that demonstrates the full path: real Commanded
(`Commanded.EventStore.Adapters.InMemory` for fast iteration), real
Ecto, real read model. The projection module itself is under 70 lines
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

### Seeing a projection in LiveDashboard

A projection is a Broadway topology, so
[`broadway_dashboard`](https://hexdocs.pm/broadway_dashboard) renders one
with no work from Scriba — it discovers pipelines through
`Broadway.all_running/0` and handles Scriba's `{:via, Registry, ...}` names:

```elixir
# deps
{:broadway_dashboard, "~> 0.4"}

# router
live_dashboard "/dashboard",
  additional_pages: [broadway: BroadwayDashboard]
```

It shows the producer, processors and batchers, their concurrency, and
successful/failed counts per stage. Scriba ships no dashboard of its own:
the ecosystem distributes operational UIs as companion packages, and this
one already works. `bench/test/broadway_dashboard_spike_test.exs` keeps that
claim honest.

---

## How far along, and how far behind

`Scriba.info/1` reports two numbers an operator can alert on:

```elixir
{:ok, info} = Scriba.info(MyApp.Projections.Orders)

info.watermark   # 48_213 — every event up to here is accounted for
info.lag_ms      # 1_240  — the event at that position happened 1.2s ago
```

The **watermark** is the contiguous global position: every event at or below
it has been committed, skipped or dead-lettered, with no gap underneath. That
is the number a replica could resume from, and the one that says how far a
rebuild has got. Per-stream cursors cannot answer either question — a minimum
across them ignores streams the projection never wrote to, and a maximum
counts work sitting above an event still in flight.

**Lag** is measured from the event's own timestamp rather than from the event
store's head, which Commanded's adapter behaviour does not expose. An idle,
fully caught-up projection therefore reports the age of the last event it
saw, which is what you want when asking whether anything is still flowing.

Both are written outside the commit transaction and throttled to roughly one
write a second while events are in flight, flushing immediately once the
projection catches up. They can lag what was applied; they cannot run ahead
of it. See `Scriba.Watermark` for why that direction is the safe one.

### Running on more than one node

Every node runs the same supervision tree, so every node tries to start the
projection — and a persistent subscription admits one subscriber. Scriba
treats that as normal: one node acquires the subscription and projects, the
others stand by, retrying about once a minute, and take over when the holder
goes away. No leader election, no extra dependency, nothing to configure.

The one thing to configure is `:subscription_name`, which defaults to
`"scriba"`. That default is per subscription, not per projection, so **two
different projections left on it contend with each other**: one acquires the
name and the other stands by indefinitely, reporting `:running` while
projecting nothing. Give each projection its own name.

```elixir
source: {Scriba.Source.Commanded, application: MyApp, subscription_name: "orders"}
```

```
[:scriba, :source, :standby]      # waiting; another subscriber holds the name
[:scriba, :source, :subscribed]   # acquired it, including after a takeover
```

A standby's projection reports `:running` — its pipeline is up and healthy,
it simply has no subscription yet — so those two events, not `info/1`, are
what tell you which node is doing the work.

The same behaviour covers a cutover from `commanded_ecto_projections`: start
Scriba while the old projector still holds the name, and it picks up the
moment you stop it.

---

## Reading the dead-letter table

Dead letters outlive the projection that produced them, so these read from
the table rather than from a running process — which is the point, since a
halted or stopped projection is when you go looking.

```elixir
Scriba.dead_letter_stats(MyApp.Projections.Orders)
#=> %{total: 143,
#     by_error_kind: %{"commit:23505 (unique_violation)" => 140,
#                      "Elixir.ArgumentError" => 3},
#     oldest: ~U[2026-09-16 09:12:03Z], newest: ~U[2026-09-16 11:40:55Z]}

Scriba.dead_letters(MyApp.Projections.Orders, limit: 10)
Scriba.dead_letters(MyApp.Projections.Orders, stream_id: "order-42", order: :asc)
```

The distribution is the diagnosis. One kind on one stream is a poison event.
One kind spread across every stream is a schema or handler problem that
dead-lettering is papering over — and if a whole batch fails that way at
once, `Scriba.Circuit` halts the projection rather than draining the stream
into the table.

A row carries `:id`, `:position`, `:stream_id`, `:event_type`,
`:error_kind`, `:error_message`, `:occurred_at` and the serialized
`:event_data`. `:id` orders rows that share a timestamp and is the natural
paging key alongside `:limit` and `:offset`. There is
no replay function: `:event_data` records what failed rather than a value
that can be re-dispatched, so replaying means reading the event from the
source by `:position`. Filters, paging and ordering are in
`Scriba.DeadLetter.list/3`.

---

## Testing your projections

`Scriba.Testing` runs a projection's `handle/2` clauses and commits the
results through its configured target, without starting a pipeline — so a
test asserts on read-model rows rather than on handler return values.

```elixir
test "a deposit increases the balance" do
  Scriba.Testing.project(MyApp.Projections.Balances, [
    %AccountOpened{account_id: "acc-1"},
    %Deposited{account_id: "acc-1", amount_cents: 500}
  ])

  assert Repo.get(Balance, "acc-1").balance_cents == 500
end
```

`project/3` returns a `Scriba.Testing.Result` with what committed, what the
handler skipped, what it failed on, and what it returned that the target
cannot apply. Events run in one transaction, in order, and per-stream cursors
advance exactly as they would in production:

```elixir
Scriba.Testing.project(MyProjection, [
  {%Deposited{}, stream_id: "acc-1"},
  {%Deposited{}, stream_id: "acc-2"}
])
```

`Scriba.Testing.handle/3` calls a single clause with no database at all, for
asserting the shape a handler returns — including `:skip` for event types the
projection ignores.

It exercises the handler and the commit, not the pipeline around them: no
retries, no dead-letter routing, no dedup, no telemetry. Handler failures come
back to the caller instead of being routed, so a test can assert on them
directly.

---

## Running Scriba's own tests

| Command | What runs | Requires |
|---|---|---|
| `mix test.fast` | Non-property tests | — |
| `mix test.property_db` | Real-Postgres property tests in `test/property_db/` | `SCRIBA_TEST_DB_*` env vars |
| `mix test.all` | Everything ExUnit will run with the current environment | — for always-runnable parts; env vars for `property_db` |

`mix test.all` is the canonical full-suite command. Which command
produced a result matters: "fast suite: 99/0" and "full suite: 99/0"
describe different coverage, and a bare "99 tests, 0 failures" does not
say whether the real-Postgres tests ran at all.

### Real-Postgres property tests

Property tests in `test/property_db/` need a live Postgres. Set all five
of:

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
- [`REBUILDING.md`](REBUILDING.md) — rebuilding a read model from history:
  the `(name, version)` side-by-side procedure, watching progress, cutting
  over, and the two things that bite (side effects replay; dead letters are
  not replayed).
- [`SCRIBA_ARCHITECTURE.md`](SCRIBA_ARCHITECTURE.md) — the
  architectural contract. Read this before opening a PR that
  changes engine behavior.
- [`examples/bank/README.md`](https://github.com/thatsme/scriba/blob/main/examples/bank/README.md)
  — example app walkthrough.
- `Scriba.Telemetry` moduledoc — the full telemetry event surface.

---

## Versioning

Scriba follows semver. The public API is `start_projection`, `pause`,
`resume`, `stop`, `info`, `list`, `dead_letters`, `dead_letter_stats` and
`reset` in `Scriba`, plus the macro at `Scriba.Projection` and the
test-time helpers in `Scriba.Testing`. The six lifecycle functions frozen
at v0.1.0 have not changed; 0.2.0 added the last three, which are
additive.

0.2.0 adds a table (`scriba_watermarks`), so upgrading from 0.1.x means one
migration — `Scriba.Migrations.up(from: 1)`. Nothing else breaks.

`Scriba.Target` and `Scriba.Source` behaviours are not frozen, and will
widen if other sources or targets are built. Custom adapter authors should
pin against a specific minor version.

Only Commanded and Ecto/Postgres ship today, and nothing else is being
prepared for speculatively — an interface with one implementation behind it
encodes that implementation's assumptions. **If you need another source or
target, open an issue**: it gets built with you, and the second
implementation is what reveals the right shape. One constraint is worth
knowing up front, because it is the guarantee rather than a detail: a target
commits read-model rows, cursor advances and dead letters in a single
transaction, so a store that cannot do that cannot provide effectively-once
delivery.

---

## License

Apache-2.0. See [LICENSE](https://github.com/thatsme/scriba/blob/main/LICENSE).

---

## Contributing

Issues and PRs welcome at https://github.com/thatsme/scriba.
Architecture-affecting changes should reference the relevant section
of `SCRIBA_ARCHITECTURE.md`; deliberate departures from the contract
require documentation in the PR explaining why.
