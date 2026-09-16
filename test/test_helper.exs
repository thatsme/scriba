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

    # Configured is not the same as reachable, and the difference used to take
    # the whole suite down.
    #
    # config/test.exs gates on the five env vars being *present*. A developer
    # with .env.local on disk therefore always lands in this branch — and
    # start_link/0 above succeeds even with no database, because Ecto pools
    # connect lazily. The first real I/O is the migrator below, which raised.
    # That happens in test_helper.exs, before ExUnit runs anything, so it was
    # not "the property_db tests fail": every invocation died at boot,
    # including `mix test.fast`, whose whole purpose is to need no database.
    #
    # Stopping the container was enough to trigger it, and the cause is
    # invisible in git — a fresh session would see a connection error and
    # start debugging Scriba.
    #
    # So probe first and degrade to the same clean skip that unset vars get.
    reachable? =
      try do
        Ecto.Adapters.SQL.query!(Scriba.Test.Repo, "SELECT 1", [], timeout: 2_000)
        true
      rescue
        _ -> false
      catch
        :exit, _ -> false
      end

    # Run the test-suite migrations:
    #   1 — Scriba's own scriba_positions + scriba_dead_letters via the
    #       CreateScribaTables module that delegates to Scriba.Migrations.up/0.
    #       Same integration shape a real user gets.
    #   2 — test_read_models for property_db handlers' read-model writes.
    # Idempotent: Ecto.Migrator skips versions already tracked in
    # `schema_migrations`.
    if reachable? do
      Ecto.Migrator.run(
        Scriba.Test.Repo,
        [
          {1, Scriba.Test.Migrations.CreateScribaTables},
          {2, Scriba.Test.Migrations.CreateTestReadModels},
          {3, Scriba.Test.Migrations.CreateScribaWatermarks}
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
    else
      ExUnit.configure(exclude: [property_db: true])

      IO.puts(:stderr, """

      Real-Postgres property tests are excluded: SCRIBA_TEST_DB_* are set, but
      the database is not reachable.

        host: #{System.get_env("SCRIBA_TEST_DB_HOST")}:#{System.get_env("SCRIBA_TEST_DB_PORT")}
        name: #{System.get_env("SCRIBA_TEST_DB_NAME")}

      Start it, or unset the variables / remove .env.local to silence this.
      Everything else runs normally.
      """)
    end
end
