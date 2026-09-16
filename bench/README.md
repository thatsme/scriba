# Scriba throughput benchmark

Measures how fast a projection catches up on history it has never seen —
the number that decides whether a read-model rebuild or a post-outage
catch-up takes minutes or days.

It runs against a **real** event store: Commanded backed by
[`eventstore`](https://hex.pm/packages/eventstore) on Postgres, not the
InMemory adapter. That distinction is the point. InMemory and EventStore
differ in subscription delivery and acknowledgement semantics, and a
projection engine can be correct against one and broken against the other.

## Setup

The repository-root `docker-compose.yml` provides Postgres on port 5433:

```sh
docker compose up -d          # from the repository root
cd bench
mix deps.get
mix bench.setup               # creates and initializes both databases
```

`mix bench.setup` creates `scriba_bench_events` (the event store) and
`scriba_bench_read` (the read model, holding Scriba's own tables). For a
Postgres elsewhere, copy `.env.local.example` to `.env.local` and set the
`SCRIBA_BENCH_DB_*` variables; it is auto-loaded, and real shell environment
variables take precedence.

## Running

```sh
mix bench.throughput                                    # 2000 events, 50 streams
mix bench.throughput --events 5000 --streams 100
mix bench.throughput --events 5000 --source-opt buffer_size=500
mix bench.throughput --reseed                           # append a fresh batch first
```

Output:

```
5000 events projected in 2042ms
Throughput: 2448.6 events/sec

Reference points at this rate:
  1M events:  6.8 min
  10M events: 68.1 min
```

Each run subscribes under a name never used before and truncates the read
model and cursor tables first, so runs are independent and repeatable. Seeding
is not timed.

## What it measures, and what it does not

The handler does one insert per event — no queries, no computation — so the
result is an upper bound on the delivery path: subscription, batching, commit,
acknowledgement. A real projection's handler work comes out of this budget.

It measures **catch-up**, reading history that already exists. Steady-state
throughput against a live stream of writes is a different question and this
harness does not answer it.

`--source-opt k=v` forwards options to the subscription, which is what makes
before/after comparisons possible. An option the engine does not forward
reaches nothing, and that is itself a result worth recording.

## Results

Recorded on 5,000 events over 100 streams, Postgres 16 in Docker, one machine,
`parallelism: 8`:

| `:buffer_size` | Throughput | 10M events |
|---|---|---|
| unset (adapter default, 1) | 9.1 events/sec | 12.7 days |
| 500 | 2,448 events/sec | 68 minutes |

The adapter default is one in-flight event per subscriber. Acknowledgement
happens after the batch commits, so a batcher holding a single event waits out
its whole `:batch_timeout` before acking and releasing the next one —
`:parallelism` cannot raise a ceiling imposed upstream of it. See
`Scriba.Source.Commanded` for the tradeoff that comes with a larger buffer.

Absolute numbers are hardware- and configuration-specific. The ratio between
rows is the durable finding.

## Why a separate project

`bench/` is its own Mix project so that `eventstore` and
`commanded_eventstore_adapter` stay out of the library's dependency tree. It
depends on Scriba by path, so it measures the working tree rather than a
published release.
