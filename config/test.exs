import Config

# Real-Postgres test infrastructure. Property_db tests
# require a live Postgres reachable via these environment variables. The
# fast suite (mix test.fast) does NOT require any of this.
#
# Policy:
#   * All five vars set → configure Scriba.Test.Repo. Tests in test/property_db/
#     run normally.
#   * All five vars unset → leave config absent. test_helper.exs detects the
#     absence, excludes :property_db tagged tests, and prints a clear
#     skip-with-instructions message.
#   * Partial config → raise here with a diagnostic naming the missing vars.
#     Treating partial config as a misconfiguration prevents silent fall-back
#     to nil credentials.
#
# Local dev convenience: a `.env.local` file in the project root is loaded
# here (if present) before the env-var checks below. `.env.local` is
# gitignored; `.env.local.example` is committed as a template. Real shell
# environment variables still win — `.env.local` only fills in vars that
# aren't already set.

env_local_path = Path.expand("../.env.local", __DIR__)

if File.exists?(env_local_path) do
  env_local_path
  |> File.read!()
  |> String.split("\n", trim: true)
  |> Enum.each(fn line ->
    trimmed = String.trim(line)

    cond do
      trimmed == "" ->
        :ok

      String.starts_with?(trimmed, "#") ->
        :ok

      true ->
        case String.split(trimmed, "=", parts: 2) do
          [key, value] ->
            key = String.trim(key)
            value = value |> String.trim() |> String.trim("\"") |> String.trim("'")

            # Don't overwrite a real shell env var.
            if System.get_env(key) in [nil, ""] do
              System.put_env(key, value)
            end

          _ ->
            :ok
        end
    end
  end)
end

required = ~w(SCRIBA_TEST_DB_HOST SCRIBA_TEST_DB_PORT SCRIBA_TEST_DB_NAME SCRIBA_TEST_DB_USER SCRIBA_TEST_DB_PASS)

values = Enum.map(required, fn var -> {var, System.get_env(var)} end)
present = Enum.filter(values, fn {_, v} -> v not in [nil, ""] end)
missing = Enum.filter(values, fn {_, v} -> v in [nil, ""] end) |> Enum.map(&elem(&1, 0))

cond do
  present == [] ->
    # No vars set — property_db tests will skip via test_helper.exs.
    :ok

  missing == [] ->
    # All vars set — configure the repo.
    [host, port_str, db, user, pass] = Enum.map(values, &elem(&1, 1))

    config :scriba, Scriba.Test.Repo,
      pool: Ecto.Adapters.SQL.Sandbox,
      hostname: host,
      port: String.to_integer(port_str),
      database: db,
      username: user,
      password: pass,
      log: false

  true ->
    # Partial config — fail fast with a useful diagnostic.
    raise """
    Scriba real-Postgres test config is partial.

    Missing: #{Enum.join(missing, ", ")}

    Set ALL of #{Enum.join(required, ", ")} to enable property_db tests,
    or unset all of them to skip them. Mixed state is a misconfiguration.
    """
end
