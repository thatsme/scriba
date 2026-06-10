import Config

# Local-dev convenience: auto-load a `.env.local` file (if present) from the
# example root BEFORE reading the BANK_DEMO_DB_* vars below. This mirrors the
# main project's config/test.exs so the README's bare `mix bank.setup &&
# mix bank.demo` works against a non-localhost Postgres with no shell setup.
#
# `.env.local` is gitignored; `.env.local.example` is the committed template.
# Real shell environment variables always win — `.env.local` only fills in
# vars that aren't already set, so `BANK_DEMO_DB_HOST=... mix bank.demo`
# still overrides the file.

env_local_path = Path.expand("../.env.local", __DIR__)

if File.exists?(env_local_path) do
  env_local_path
  |> File.read!()
  |> String.split("\n", trim: true)
  |> Enum.each(fn line ->
    trimmed = String.trim(line)

    cond do
      trimmed == "" or String.starts_with?(trimmed, "#") ->
        :ok

      true ->
        case String.split(trimmed, "=", parts: 2) do
          [key, value] ->
            key = String.trim(key)
            value = value |> String.trim() |> String.trim("\"") |> String.trim("'")

            # Don't overwrite a real shell env var.
            if System.get_env(key) in [nil, ""], do: System.put_env(key, value)

          _ ->
            :ok
        end
    end
  end)
end

# Runtime DB config: read creds from BANK_DEMO_DB_* env vars when present
# (set in the shell, or loaded from .env.local above), else fall back to the
# config.exs defaults (localhost / port 5432 / database bank_demo / postgres
# / postgres).
#
# A practitioner cloning the repo and running `mix bank.demo` against a local
# Postgres gets a working demo without setting anything. Users on non-standard
# setups copy .env.local.example → .env.local and customize — it is auto-loaded.

if System.get_env("BANK_DEMO_DB_HOST") do
  config :bank, Bank.Repo,
    hostname: System.fetch_env!("BANK_DEMO_DB_HOST"),
    port: String.to_integer(System.get_env("BANK_DEMO_DB_PORT") || "5432"),
    database: System.fetch_env!("BANK_DEMO_DB_NAME"),
    username: System.fetch_env!("BANK_DEMO_DB_USER"),
    password: System.fetch_env!("BANK_DEMO_DB_PASS"),
    pool_size: 10
end
