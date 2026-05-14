import Config

# Runtime config: read DB creds from BANK_DEMO_DB_* env vars when
# present, else fall back to the dev.exs defaults (localhost / port
# 5432 / database bank_demo / postgres / postgres).
#
# A practitioner cloning the repo and running `mix bank.demo` against
# a local Postgres should get a working demo without setting anything.
# Users on non-standard setups copy .env.local.example → .env.local and
# customize.

if System.get_env("BANK_DEMO_DB_HOST") do
  config :bank, Bank.Repo,
    hostname: System.fetch_env!("BANK_DEMO_DB_HOST"),
    port: String.to_integer(System.get_env("BANK_DEMO_DB_PORT") || "5432"),
    database: System.fetch_env!("BANK_DEMO_DB_NAME"),
    username: System.fetch_env!("BANK_DEMO_DB_USER"),
    password: System.fetch_env!("BANK_DEMO_DB_PASS"),
    pool_size: 10
end
