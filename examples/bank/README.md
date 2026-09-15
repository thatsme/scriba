# Scriba Bank Demo

A self-contained example that exercises Scriba end-to-end against
real Commanded: aggregate dispatches commands → events fire → Scriba
projects them into a Postgres read model.

The shortest path to seeing Scriba work.

## What this demonstrates

- The five-line `use Scriba.Projection` API in real use.
  See [`lib/bank/projections/balances.ex`](lib/bank/projections/balances.ex)
  for the projection module the demo runs against — it's about 30 lines
  total including comments.
- Real Commanded — events flow through `Commanded.Application`'s
  subscription machinery, the same path a production app uses.
- Per-stream ordering: 3 accounts get interleaved deposits and
  withdrawals; the read model converges to the correct balance for
  each account, proving events on each stream are applied in order.
- Atomic read-model + cursor updates via `Scriba.Target.Ecto` — the
  Multi commit guarantees you never see a state where the balance
  advanced but Scriba's cursor didn't, or vice versa.

## What this doesn't demonstrate

- **Persistence of the event store.** The Commanded event store uses
  `Commanded.EventStore.Adapters.InMemory` — restarting the demo
  forgets everything. See "Persistent event store" below for the
  10-line swap to `commanded_eventstore_adapter`.
- Crash recovery / cursor resume. Scriba's PD3 property test covers
  this; the bank demo is about the happy path.
- Dead-letter routing and retries. Both are exercised by Scriba's
  test suite; this demo keeps the output clean.

## Setup

```sh
cd examples/bank
mix deps.get
mix bank.setup   # creates the database and runs migrations
```

Without any configuration, the setup task assumes a local Postgres on
port 5432 with user `postgres`/`postgres` and creates a database named
`bank_demo`. To use the Postgres from the repository-root
`docker-compose.yml` instead (published on 5433), copy
`.env.local.example` to `.env.local` — its values already match. For
any other Postgres, set the `BANK_DEMO_DB_*` variables there. The example
**auto-loads `.env.local`** if present (via `config/runtime.exs`, the
same pattern the main test suite uses) — you don't need to export the
variables into your shell. A real shell environment variable still wins,
so `BANK_DEMO_DB_HOST=… mix bank.demo` overrides the file.

## Run the demo

```sh
mix bank.demo
```

Expected output:

```
=== Scriba Bank Demo ===

Opened 3 accounts.
Dispatched 50 deposit/withdraw commands in 12ms
Waiting for projection to catch up...
Caught up in 31ms.

account_balances:
  4f3e8a07... balance: 1840.50 USD  (last event: pos 47)
  8b2c9d12... balance: 927.25 USD  (last event: pos 42)
  e1a7f0a5... balance: -312.00 USD  (last event: pos 51)

Total events processed: 53
Projection state:       running
Safe position:          42
```

The exact numbers vary — the deposit/withdraw amounts are random.
Negative balances are expected and intentional; the demo does not
enforce an overdraft check.

`Total events processed` (53 = 3 opens + 50 ops) is the count the
projection handler committed. `Safe position` is the **minimum** cursor
across the three account streams — it sits below the total because no
single stream holds all 53 events, and it's the position a fresh replica
could resume from without missing anything. Here that's 42, the lowest
of the three per-account positions above.

## What each file does

```
lib/
├── bank.ex                          (not present — no top-level glue)
├── bank/
│   ├── application.ex               # supervision tree
│   ├── commanded_app.ex             # Commanded.Application
│   ├── repo.ex                      # Ecto repo for the read model
│   ├── events.ex                    # AccountOpened / Deposited / Withdrew
│   ├── commands.ex                  # OpenAccount / Deposit / Withdraw
│   ├── account.ex                   # aggregate (validates account-must-exist)
│   ├── router.ex                    # Commanded router
│   ├── read_models.ex               # Ecto schema for account_balances
│   └── projections/
│       ├── balances.ex              # ★ the projection module — uses Scriba.Projection
│       └── starter.ex               # calls Scriba.start_projection at app boot
└── mix/
    └── tasks/
        ├── bank.demo.ex             # the main demo orchestration
        └── bank.reset.ex            # explicit truncate (demo auto-resets)

priv/
└── repo/
    └── migrations/
        ├── 20260514000001_install_scriba.exs       # Scriba.Migrations.up()
        └── 20260514000002_create_account_balances.exs
```

## Run `mix bank.reset` between custom experiments

`mix bank.demo` auto-truncates the read model on each run — useful so
the output reflects "this is what just happened" rather than
"this is what happened across N invocations."

If you want to manually inspect read-model state between custom
command dispatches (e.g. via `iex -S mix`), `mix bank.reset` truncates
`account_balances`, `scriba_positions`, and `scriba_dead_letters`
without running the demo.

## Persistent event store

The InMemory adapter is great for a quick demo but loses events on
restart. To swap to persistent storage with
[`commanded_eventstore_adapter`](https://github.com/commanded/commanded-eventstore-adapter):

1. Add the dep to `mix.exs`:
   ```elixir
   {:commanded_eventstore_adapter, "~> 1.4"}
   ```

2. Update `config/config.exs`:
   ```elixir
   config :bank, Bank.CommandedApp,
     event_store: [
       adapter: Commanded.EventStore.Adapters.EventStore,
       event_store: Bank.EventStore,
       serializer: Commanded.Serialization.JsonSerializer
     ]

   config :bank, Bank.EventStore,
     # ...event store DB connection config...
   ```

3. Define the event store module and run its migrations alongside the
   read-model migrations.

The projection module, the aggregate, the commands/events, and the
read model schema all stay identical. Scriba doesn't care which
adapter Commanded uses — it subscribes via `Commanded.EventStore.subscribe_to`,
which routes through whatever's configured.

## Where to look next

- [`SCRIBA_ARCHITECTURE.md`](../../SCRIBA_ARCHITECTURE.md) — the
  architectural contract for Scriba itself.
- [`lib/bank/projections/balances.ex`](lib/bank/projections/balances.ex) —
  the projection. The five-line API in real use.
- Scriba's main [`README.md`](../../README.md) for the full API surface.
