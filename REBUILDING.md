# Rebuilding a read model

A read model's shape changes: a column is added, a bug in a handler has been
writing the wrong total for a month, a new field has to be backfilled from
history. The read model has to be rebuilt from the event stream.

Scriba has no `rebuild/2` function, and this guide is the reason why. The
mechanism is already there — a projection's identity is `(name, version)`,
and two versions run side by side against the same events, writing to
different tables. Rebuilding is an operational procedure, not a library
call.

## The procedure

### 1. Declare the new version

```elixir
defmodule MyApp.Projections.OrdersV2 do
  use Scriba.Projection,
    name: "orders",          # same name
    version: 2,              # new version
    source: {Scriba.Source.Commanded, application: MyApp.CommandedApp},
    target: {Scriba.Target.Ecto, repo: MyApp.Repo},
    parallelism: 16

  def handle(%OrderPlaced{} = event, _meta) do
    {:insert, %OrderReadModelV2{id: event.order_id, status: "pending"}}
  end
end
```

Same `name`, new `version`, new read-model table. The name is what ties the
two together as the same logical projection; the version is what keeps their
cursors, watermarks and dead letters separate. Nothing is shared, so v2
cannot disturb v1.

### 2. Start it from the beginning

```elixir
Scriba.start_projection(MyApp.Projections.OrdersV2,
  source: {Scriba.Source.Commanded,
           application: MyApp.CommandedApp,
           subscription_name: "orders-v2",   # its own subscription
           start_from: :origin,
           buffer_size: 500}
)
```

Two details matter here.

**Its own `subscription_name`.** A persistent subscription admits one
subscriber; sharing the name with v1 means one of them fails to start. The
default is `"scriba"`, so give every projection against the same Commanded
application an explicit name.

**`buffer_size`.** The adapter default is one in-flight event, which caps
catch-up at roughly 9 events/sec — a million events would take over a day.
See the README's throughput section.

### 3. Watch it catch up

```elixir
{:ok, info} = Scriba.info(MyApp.Projections.OrdersV2)

info.watermark   # 412_908 — the contiguous position it has reached
info.lag_ms      # 8_412_000 — the event there happened 2.3 hours ago
```

`lag_ms` shrinking towards seconds is the progress signal. `[:scriba,
:projection, :lag]` reports the same pair on a timer, so a rebuild can be
watched from wherever telemetry already goes.

There is no percentage, because Scriba cannot compute one: Commanded's
adapter behaviour exposes no head position, so "how many events are there
in total" is not a question the source can answer. Falling lag is the
honest form of the same information.

### 4. Cut over

When the lag is small, point reads at the new table. That switch belongs to
the application — a config flag, a feature toggle, a deploy — because only it
knows what a consistent moment to switch looks like.

Then retire v1:

```elixir
:ok = Scriba.stop(MyApp.Projections.OrdersV1)
{:ok, _} = Scriba.reset("orders", version: 1, repo: MyApp.Repo)
```

`reset/2` clears the version's cursors and watermark. It accepts a stopped
projection and refuses a running one, and it does not touch the read model —
drop or truncate the old table yourself.

`stop/1` does not remove the projection: its Coordinator stays registered in
a terminal `:stopped` state, which is why `reset/2` still finds it. To start
the same `(name, version)` again — rather than a new version — remove the
supervision child first:

```elixir
DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
```

## Two warnings

### Side effects replay

A handler that sends email, calls an API, or publishes to a queue will do it
again for every event in history. A rebuild is not a dry run.

Scriba cannot detect this: a handler returning `{:multi, _}` can do anything
inside it. If a projection has side effects, gate them on a flag the rebuild
turns off, or move them out of the handler and drive them from
`[:scriba, :projection, :event, :stop]` telemetry instead, where a rebuild
can be filtered by projection version.

### Dead letters are not replayed

A rebuild reprocesses events from the source, so events that were
dead-lettered on the old version are processed again by the new one — and if
the handler still cannot apply them, they are dead-lettered again under the
new version. Check afterwards:

```elixir
Scriba.dead_letter_stats(MyApp.Projections.OrdersV2)
```

A rebuild that ends with the same error kinds as the old version fixed
nothing.

## What Scriba deliberately does not do

**Shadow targets and an atomic swap.** A "shadow target" is a second version
writing to a second table, which is what step 1 already describes. The swap
is the application choosing which table to read, and it is the application
that knows when that is safe. Putting either inside the library would mean
Scriba deciding when your reads can move, with less information than you
have.

**Enforcing errors during a rebuild.** Other projection engines skip poison
events while live and stop on them while rebuilding, on the grounds that a
rebuild should be all-or-nothing. Scriba treats both the same: dead-letter
and continue. The distinction is a real one and may arrive later; today,
`dead_letter_stats/2` after the rebuild is how you find out whether it was
clean.

**Progress as a percentage.** Covered above: the source cannot say how much
history exists, so any percentage would be a guess presented as a number.
