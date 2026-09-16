import Config

config :scriba_bench, ecto_repos: [ScribaBench.Repo]
config :scriba_bench, event_stores: [ScribaBench.EventStore]

config :scriba_bench, ScribaBench.CommandedApp,
  event_store: [
    adapter: Commanded.EventStore.Adapters.EventStore,
    event_store: ScribaBench.EventStore
  ]

# Connection defaults target the repository-root docker-compose.yml, which
# publishes Postgres on 5433. runtime.exs overrides from SCRIBA_BENCH_DB_*.
config :scriba_bench, ScribaBench.Repo,
  hostname: "localhost",
  port: 5433,
  database: "scriba_bench_read",
  username: "postgres",
  password: "postgres",
  pool_size: 10

config :scriba_bench, ScribaBench.EventStore,
  serializer: EventStore.JsonSerializer,
  hostname: "localhost",
  port: 5433,
  database: "scriba_bench_events",
  username: "postgres",
  password: "postgres",
  pool_size: 10

# The benchmark prints its own numbers; engine logs would interleave with
# them. :warning still surfaces halts and dead letters.
config :logger, level: :warning
