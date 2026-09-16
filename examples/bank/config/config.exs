import Config

config :bank, ecto_repos: [Bank.Repo]

# Configure Commanded with the InMemory adapter. No serializer — events
# round-trip as Elixir structs. The persistent commanded_eventstore_adapter
# would slot in here with a JsonSerializer; see README "Persistent event
# store" for the swap.
config :bank, Bank.CommandedApp, event_store: [adapter: Commanded.EventStore.Adapters.InMemory]

# Silence query logs so the demo output stays clean. Users debugging
# their own projection can bump this to :debug.
config :bank, Bank.Repo, log: false

config :logger, level: :info

# Default DB config — runtime.exs overrides from BANK_DEMO_DB_* env
# vars when present.
config :bank, Bank.Repo,
  hostname: "localhost",
  port: 5432,
  database: "bank_demo",
  username: "postgres",
  password: "postgres",
  pool_size: 10
