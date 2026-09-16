import Config

# Same .env.local convention as the main test suite and the bank example:
# a gitignored file fills in vars that aren't already set in the shell.
env_local_path = Path.expand("../.env.local", __DIR__)

if File.exists?(env_local_path) do
  env_local_path
  |> File.read!()
  |> String.split("\n", trim: true)
  |> Enum.each(fn line ->
    trimmed = String.trim(line)

    unless trimmed == "" or String.starts_with?(trimmed, "#") do
      case String.split(trimmed, "=", parts: 2) do
        [key, value] ->
          key = String.trim(key)
          value = value |> String.trim() |> String.trim("\"") |> String.trim("'")
          if System.get_env(key) in [nil, ""], do: System.put_env(key, value)

        _ ->
          :ok
      end
    end
  end)
end

if host = System.get_env("SCRIBA_BENCH_DB_HOST") do
  port = String.to_integer(System.get_env("SCRIBA_BENCH_DB_PORT") || "5433")
  user = System.get_env("SCRIBA_BENCH_DB_USER") || "postgres"
  pass = System.get_env("SCRIBA_BENCH_DB_PASS") || "postgres"

  config :scriba_bench, ScribaBench.Repo,
    hostname: host,
    port: port,
    username: user,
    password: pass

  config :scriba_bench, ScribaBench.EventStore,
    hostname: host,
    port: port,
    username: user,
    password: pass
end
