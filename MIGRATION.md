# Migrating from `commanded_ecto_projections`

For teams already running
[`commanded_ecto_projections`](https://github.com/commanded/commanded-ecto-projections)
in production. This page is about your existing projector modules and your
existing cursor — not about why Scriba exists.

---

## The question that decides your maintenance window

**You do not have to rebuild your read models. You can cut over in place.**

Both libraries track the *same number*: Commanded's global `event_number`
from an `:all` subscription.

| | `commanded_ecto_projections` | Scriba |
|---|---|---|
| Table | `projection_versions` | `scriba_positions` |
| Key | `projection_name` (one row per projector) | `(projection_name, projection_version, stream_id)` — one row **per stream** |
| Cursor column | `last_seen_event_number` (bigint) | `position` (bigint) |
| Value stored | `metadata.event_number` | `RecordedEvent.event_number` |

Same numbering space. Your old `last_seen_event_number` is therefore a
valid starting point for Scriba — you hand it to the source as
`:start_from` and the new projection picks up exactly where the old one
stopped.

The granularity differs (one global cursor vs. one cursor per stream), but
that only affects *redelivery dedup*, not where you start. Scriba builds
its per-stream rows as it goes.

### Cutover, the reversible way

1. Read your old cursor:

   ```sql
   SELECT projection_name, last_seen_event_number FROM projection_versions;
   ```

2. Stop the old projector (remove it from your supervision tree).

3. Start Scriba with a **new** subscription name and the old cursor:

   ```elixir
   use Scriba.Projection,
     name: "example_projection",
     source: {Scriba.Source.Commanded,
       application: MyApp.CommandedApp,
       subscription_name: "scriba-example_projection",
       start_from: 41_337},                    # <- last_seen_event_number
     target: {Scriba.Target.Ecto, repo: MyApp.Projections.Repo},
     parallelism: 16
   ```

`start_from: N` means *deliver events with `event_number > N`* — precisely
what `last_seen_event_number` means. Nothing replays, nothing is skipped.

Verified against `commanded_eventstore_adapter` + `eventstore`, the usual
production pairing: the adapter passes `:start_from` through unchanged, the
subscription FSM seeds its stored `last_seen` with it, sets
`last_sent = last_seen`, and catches up by reading from `last_sent + 1`.
Off-by-one in either direction here would double-apply or drop an event on
every cutover, so if you run a different adapter, confirm that boundary in
its source before trusting this procedure.

This is reversible: the old projector's subscription and its
`projection_versions` row are untouched, so rolling back is "put the old
module back in the supervision tree."

> **`:start_from` only applies when the subscription is created.** Once
> Scriba's subscription exists in the event store, its own durable
> checkpoint wins and editing `:start_from` does nothing. If you need to
> redo a cutover, delete the subscription first (e.g.
> `Commanded.EventStore.delete_subscription(App, :all, "scriba-example_projection")`).

### Alternative: reuse the old subscription

Set `subscription_name:` to the old projector's `:name`. Commanded resumes
the existing durable subscription from its acked position — no
`:start_from` needed (it would be ignored anyway).

```elixir
source: {Scriba.Source.Commanded,
  application: MyApp.CommandedApp,
  subscription_name: "example_projection"}     # the old projector's :name
```

Fewer moving parts, but **not** reversible — you have consumed the old
subscription. And the old projector must be fully stopped first: a
persistent subscription admits one subscriber, so Commanded returns
`{:error, :subscription_already_exists}`. Scriba does not fail on that: it
stands by and retries until the name is released, logging what it is waiting
for. So a cutover can start Scriba while the old projector is still running
— it picks up the moment you stop it — but nothing will be projected until
you do.

### Seeding `scriba_positions` — not needed for the path above

If you took the recommended path, **skip this section**. `start_from: N`
means the event store never delivers anything at or below `N`, so there is
nothing below the cutover point for dedup to protect against. An empty
`scriba_positions` at cutover is correct, not a gap.

Seeding matters only for the *reuse the old subscription* variant, and for
one specific reason: the old subscription's acknowledged position can lag
`last_seen_event_number`. `commanded_ecto_projections` commits the
`projection_versions` update inside its transaction, and
`Commanded.Event.Handler` acknowledges only after `handle/2` returns — so a
process death between those two points leaves the subscription checkpoint
behind the projection cursor. On resume, the events in that window are
redelivered. The old library skipped them via its own
`last_seen_event_number < event_number` guard; Scriba with no
`scriba_positions` rows has nothing to dedup against and will reprocess
them. For a non-idempotent handler (`balance + amount`) that is a double
apply.

The seed is meaningful because `scriba_positions.position` holds Commanded's
**global** `event_number`, not a per-stream version — the same number
`last_seen_event_number` holds. Writing one global value across every stream
row therefore reads correctly: "everything up to `N` is done on this stream."
What you cannot do is derive the *stream list* from `projection_versions`,
which records no streams at all — you have to enumerate them yourself:

```sql
INSERT INTO scriba_positions
  (projection_name, projection_version, stream_id, position, updated_at)
SELECT 'example_projection', 1, s.stream_id, 41337, now()
  FROM (/* your source of stream ids — see below */) AS s
ON CONFLICT DO NOTHING;
```

Source the stream ids from somewhere you trust: your event store's streams
table, or your read model if its primary key *is* the aggregate id. A stream
you miss simply gets no dedup row and is reprocessed from the subscription's
checkpoint — so under this variant, seed the streams whose handlers are not
idempotent first.

Seed narrowly rather than exhaustively. `Scriba.Position.init_cache/3`
preloads at most 10,000 rows (`ORDER BY stream_id`), and any stream beyond
that is looked up individually from Postgres on first touch, in the event
hot path. Seeding a million streams does not cost a million rows of memory
— it costs a synchronous round-trip per stream during warm-up.

### When you *do* want a rebuild

If you're changing the read-model shape, don't migrate the cursor — bump
`:version` and run the new projection alongside the old one from
`:origin`, then swap reads when it catches up. That is what the
`(name, version)` identity is for, and it needs no maintenance window
either.

---

## What happens to `projection_versions`

Nothing — Scriba never reads or writes it. It is left exactly as the old
library left it.

Keep it until the cutover is proven (it is your rollback state). Drop it
afterwards in your own migration. `Scriba.Migrations.up/1` creates only
Scriba's own tables — `scriba_positions`, `scriba_dead_letters` and
`scriba_watermarks` — and does not touch or supersede the old table
automatically.

---

## `project/2,3` → `handle/2`

`project` was a macro that pattern-matched in its arguments and handed you
an `Ecto.Multi` to build. `handle/2` is an ordinary function — same
pattern matching, ordinary function heads, and you return a description of
the write instead of building it.

**Before:**

```elixir
defmodule MyApp.ExampleProjector do
  use Commanded.Projections.Ecto,
    application: MyApp.Application,
    repo: MyApp.Projections.Repo,
    name: "example_projection"

  project %AnEvent{name: name}, _metadata, fn multi ->
    Ecto.Multi.insert(multi, :example_projection, %ExampleProjection{name: name})
  end

  project %ItemRenamed{uuid: uuid, name: name}, fn multi ->
    Ecto.Multi.update_all(multi, :item,
      from(i in ItemProjection, where: i.uuid == ^uuid), set: [name: name])
  end
end
```

**After:**

```elixir
defmodule MyApp.ExampleProjector do
  use Scriba.Projection,
    name: "example_projection",
    source: {Scriba.Source.Commanded, application: MyApp.Application},
    target: {Scriba.Target.Ecto, repo: MyApp.Projections.Repo},
    parallelism: 16

  def handle(%AnEvent{name: name}, _meta) do
    {:insert, %ExampleProjection{name: name}}
  end

  def handle(%ItemRenamed{uuid: uuid, name: name}, _meta) do
    {:update, ItemProjection, [uuid: uuid], set: [name: name]}
  end

  def handle(_event, _meta), do: :skip     # <- REQUIRED. See below.
end
```

### The catch-all is not optional

`Commanded.Event.Handler` generates `def handle(_event, _metadata), do: :ok`
for you, so events your projector didn't `project` were silently ignored.

**Scriba generates no such fallback.** An event with no matching clause
raises `FunctionClauseError`, which Scriba converts into a dead-letter row
and advances the cursor past it. On a busy `:all` subscription that means
every unrelated event in your system lands in `scriba_dead_letters`.

Add `def handle(_event, _meta), do: :skip` as the last clause of every
migrated projector.

This is the most *common* migration mistake, but not the most dangerous —
it fails loudly into a table the checklist already tells you to watch. The
mistake that fails quietly is
[cross-stream shared state](#your-handler-now-runs-in-parallel): your handler
now runs concurrently across streams, and a handler that reads-then-writes a
row shared between aggregates was implicitly safe before and is not now. That
one corrupts a read model without raising anything. Read that section before
you migrate a projector that maintains a counter, a total, or a ranking.

### Option mapping

| `commanded_ecto_projections` | Scriba |
|---|---|
| `application:` | `source: {Scriba.Source.Commanded, application: ...}` |
| `repo:` | `target: {Scriba.Target.Ecto, repo: ...}` |
| `name:` | `name:` — but see below |
| — | `parallelism:` (required, no default) |
| — | `version:` (defaults to `1`) |
| `schema_prefix:` | none — rejected at compile time, with a message saying so |
| `timeout:` | none — rejected at compile time as an unknown option |

`:name` is Scriba's *projection* identity (it keys `scriba_positions`), not
the event-store subscription name — that's `:subscription_name` on the
source, defaulting to `"scriba"`.

**If you run more than one projection against one Commanded application,
give each an explicit `:subscription_name`.** This is the most likely
first-deploy mistake, because the default is shared: the second projection
to start asks for a subscription the first already holds, and the event
store returns `{:error, :subscription_already_exists}`.

That does not fail loudly. Scriba treats a held subscription as a standby
situation — the projection starts, reports `:running`, retries in the
background, and projects nothing until the name is released. The symptom is
"one of my projections never processes anything" with a healthy-looking
status, which is harder to spot than a crash. Watch for
`[:scriba, :source, :standby]`, or read the log line it emits once.

That behaviour is deliberate, because it is also what lets you deploy Scriba
while the old projector still holds the name: Scriba waits, and picks up the
moment you stop it.

Also: if your old projector was named `"orders_v2"`, drop the suffix and
use `version: 2`. Scriba warns at compile time when `:name` matches
`_v<integer>$`.

---

## `Ecto.Multi` differences

Four of the six return shapes cover what most projectors did by hand:

| You wrote | Now return |
|---|---|
| `Ecto.Multi.insert(multi, key, struct)` | `{:insert, struct}` |
| `Ecto.Multi.update_all(multi, key, query, set: kw)` | `{:update, Schema, filter_kw, set: kw}` |
| `Ecto.Multi.delete_all(multi, key, query)` | `{:delete, Schema, filter_kw}` |
| returning `multi` unchanged (skip) | `:skip` |

Anything else — `update` with a changeset, `inc`, upserts, multi-table
writes, subqueries — goes through the escape hatch:

```elixir
def handle(%BalanceCredited{account_id: id, amount: amount}, meta) do
  {:multi,
   Ecto.Multi.update_all(Ecto.Multi.new(), {:credit, meta.id},
     from(a in Balance, where: a.id == ^id), inc: [balance: amount])}
end
```

Three things changed underneath, and all three can bite:

**1. One transaction per *batch*, not per event.** The old library ran
`Repo.transaction/2` once per event. Scriba assembles up to `:batch_size`
events (default 50, across streams) into a single `Ecto.Multi` and commits
once. Your read-model writes and the cursor advance are still atomic — but
a failure anywhere in the batch fails the whole batch, and all of its
events are redelivered.

**2. Multi keys must be unique per event.** The old docs used static atom
keys (`:example_projection`) because each event had its own transaction. In
one Scriba batch those collide. Scriba detects the duplicate operation
names before merging and dead-letters the colliding event with `error_kind`
`"multi_key_collision"`, so one bad handler does not take the batch down —
but that event's write never lands, which is not what you want either.

Key by something unique — `meta.id` is the event's UUID:

```elixir
{:multi, Ecto.Multi.insert(Ecto.Multi.new(), {:my_op, meta.id}, struct)}
```

The declarative shapes handle this for you (they key by event id).

**3. Reserved keys.** Scriba adds its own steps to the batch Multi:
`{:scriba_event, event_id}`, `{:scriba_position, stream_id}`, and
`{:scriba_dead_letter, event_id}`. Don't produce those from `{:multi, _}`.

---

## The `meta` map is not the old `metadata` map

The old library passed Commanded's *enriched* metadata — a flat map of
`RecordedEvent` fields merged with your event metadata merged with
`handler_name` / `application`. Scriba passes a fixed six-key map, with
your event metadata **nested**:

```elixir
%{
  id:          "event-uuid",      # was metadata.event_id
  stream_id:   "aggregate-uuid",  # was metadata.stream_id
  type:        "AnEvent",         # (not previously exposed)
  position:    42,                # was metadata.event_number
  metadata:    %{...},            # your event metadata, no longer flattened
  occurred_at: ~U[...]            # was metadata.created_at
}
```

Rewrites required:

| Old | New |
|---|---|
| `metadata.event_id` | `meta.id` |
| `metadata.event_number` | `meta.position` |
| `metadata.stream_id` | `meta.stream_id` |
| `metadata.created_at` | `meta.occurred_at` |
| `metadata.handler_name` | — (you know your own module) |
| `metadata[:my_key]` | `meta.metadata[:my_key]` — but see the serializer note below |
| `metadata.correlation_id` | **not available** |
| `metadata.causation_id` | **not available** |
| `metadata.stream_version` | **not available** |

**Serializer note.** Whether your metadata keys are atoms or strings is a
property of your event store's serializer, not of Scriba — both libraries
read the same `RecordedEvent.metadata`. Under `JsonSerializer` they come back
as strings, so it is `meta.metadata["my_key"]` in Scriba exactly as it was
`metadata["my_key"]` before. The mapping above is unchanged either way; only
the key type moves.

The last three are real gaps: Scriba's source copies `RecordedEvent.metadata`
verbatim rather than Commanded's enriched map, and those three are separate
`RecordedEvent` fields. If your projector reads them, you need them written
into your event metadata at dispatch time.

---

## Callbacks with no direct equivalent

**`after_update/3`** — the nearest thing is telemetry, with a caveat about
*when*. `[:scriba, :projection, :event, :stop]` fires per event but
*before* the batch commits; `[:scriba, :projection, :batch, :stop]` fires
after the commit, which is the semantic `after_update/3` had — but at batch
granularity, without the per-event `changes` map. Side effects that must
happen exactly once after a durable commit belong outside the projector
(read the read model, or use the dead-letter/telemetry stream).

**`error/3`** — replaced by the retry policy plus dead-letter routing. Where
the old handler's `error/3` typically stopped the projector on a bad event
(blocking the whole projection until someone intervened), Scriba retries
(3 attempts, exponential backoff, configurable via `:retry`) and then writes
the event to `scriba_dead_letters` and **advances past it**. Your projection
never blocks; a bad event becomes an alert, not an outage. Subscribe to
`[:scriba, :projection, :dead_letter]` and page on it.

**`schema_prefix/1,2`** — no equivalent. You can still pass
`prefix:` on your own operations via `{:multi, _}`, but `scriba_positions`
and `scriba_dead_letters` live in the repo's default prefix. Multi-tenant
projectors that relied on prefix-per-tenant should stay on the old library
for now.

**`consistency: :strong`** — **not supported.** Commanded's strong
consistency works because `Commanded.Event.Handler` registers itself with
Commanded's internal subscriptions registry, and
`dispatch(cmd, consistency: :strong)` waits for those registrations.
Scriba subscribes to the event store directly and never registers. If any of your dispatch sites use `consistency: :strong`
and depend on a projector you're migrating, that guarantee is silently lost
— the dispatch returns without waiting for Scriba. Audit your dispatch
sites before migrating such a projector.

---

## Behaviour changes worth a moment's thought

### Your handler now runs in parallel

The old projector was a single
GenServer: one event at a time, globally serial across every aggregate.
Scriba runs `:parallelism` processors, routing by `stream_id` hash. Events
on one stream stay strictly ordered; events on *different* streams now run
concurrently. Handlers that read-then-write a row shared across aggregates
(a global counter, a leaderboard) were implicitly safe before and are not
now. Per-aggregate projections — the overwhelming majority — are unaffected.

**Dead-lettered events advance the cursor.** Covered above, but it is the
change most likely to surprise on-call: the projection keeps up rather than
halting at the first poison event.

**`parallelism:` has no default** and is required. Start at 16 for a typical
projection, 4 for low-throughput; `System.schedulers_online()` for CPU-bound
handlers.

---

## Cutover checklist

1. Rewrite `project` clauses as `handle/2` clauses; **add the `:skip`
   catch-all.**
2. Rewrite metadata access per the table above.
3. Give `{:multi, _}` returns event-unique keys.
4. Audit dispatch sites for `consistency: :strong` on this projector.
5. Audit handlers for cross-stream shared state.
6. Run `Scriba.Migrations.up()` in a migration — or, if you are already
   running Scriba 0.1.x, `Scriba.Migrations.up(from: 1)` to add the
   watermark table to the schema you have.
7. Record `last_seen_event_number` from `projection_versions`.
8. Stop the old projector; deploy Scriba with `subscription_name:` (new)
   and `start_from:` (the recorded number).
9. Watch `[:scriba, :projection, :dead_letter]` and check `error_kind`. It
   tells you which rewrite mistake you made:

   | `error_kind` | What you did |
   |---|---|
   | `"Elixir.FunctionClauseError"` | missing `handle/2` clause — add the `:skip` catch-all |
   | `"invalid_return"` | returned something outside the six shapes |
   | `"multi_key_collision"` | reused a static `Ecto.Multi` key across a batch |
   | `"error"` | your handler returned `{:error, _}` |
   | `"commit:Ecto.ConstraintError (unique: …)"` | the write was rejected by a constraint. `{:insert, struct}` passes a bare struct, so Ecto raises its own error and the SQLSTATE is not preserved — the constraint name is the identifier you get |
   | `"commit:<SQLSTATE>"` | the write was rejected and the raw Postgres error survived — typically from a `{:multi, _}` return, e.g. `"commit:23502 (not_null_violation)"` |

   Also alert on `[:scriba, :source, :batch, :failed]` (batches are not
   committing) and page on `[:scriba, :projection, :halted]` (the projection
   has stopped and needs a human).
10. Keep `projection_versions` until you're satisfied, then drop it.
