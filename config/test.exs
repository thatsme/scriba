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
