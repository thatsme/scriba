ExUnit.start()

# --- Real-Postgres property test infrastructure ---
#
# config/test.exs has already enforced the all-or-nothing-or-error env var
# policy. By the time this file runs:
#   * Either Application.get_env(:scriba, Scriba.Test.Repo) returns a full
#     keyword list — env vars were all set, repo can start.
#   * Or it returns nil — env vars were all unset, property_db tests skip.
#   * Partial-config would have raised at config load; we never reach here.

case Application.get_env(:scriba, Scriba.Test.Repo) do
  nil ->
    ExUnit.configure(exclude: [property_db: true])

    IO.puts(:stderr, """

    Real-Postgres property tests are excluded.

    Set the following environment variables to enable them:
      SCRIBA_TEST_DB_HOST
      SCRIBA_TEST_DB_PORT
      SCRIBA_TEST_DB_NAME
      SCRIBA_TEST_DB_USER
      SCRIBA_TEST_DB_PASS

    The fast suite (mix test.fast) runs without any of this.
    """)

  _config ->
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    {:ok, _pid} = Scriba.Test.Repo.start_link()

    # Run the test-suite migrations:
    #   1 — Scriba's own scriba_positions + scriba_dead_letters via the
    #       CreateScribaTables module that delegates to Scriba.Migrations.up/0.
    #       Same integration shape a real user gets.
    #   2 — test_read_models for property_db handlers' read-model writes.
    # Idempotent: Ecto.Migrator skips versions already tracked in
    # `schema_migrations`.
    Ecto.Migrator.run(
      Scriba.Test.Repo,
      [
        {1, Scriba.Test.Migrations.CreateScribaTables},
        {2, Scriba.Test.Migrations.CreateTestReadModels}
      ],
      :up,
      all: true
    )

    # Sandbox in :manual mode — tests explicitly check out a connection.
    # Property_db tests use the shared-mode helper in
    # `Scriba.Test.PropertyDbHelpers.setup_sandbox/1` to make the connection
    # visible across the projection's process tree (Coordinator, Pipeline,
    # Broadway producer/processors/batchers, Ecto target transaction). See
    # `test/property_db/sandbox_harness_test.exs` for the foundation test.
    Ecto.Adapters.SQL.Sandbox.mode(Scriba.Test.Repo, :manual)
end
