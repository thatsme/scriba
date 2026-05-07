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

    # Run the projection-tables migration. The migration module
    # (Scriba.Test.Migrations.CreateScribaTables) is a proper Ecto migration
    # that delegates to Scriba.Migrations.up/0 — same integration shape a
    # real user gets when they invoke Scriba.Migrations.up/0 from their own
    # migration file. Idempotent: Ecto.Migrator skips versions already
    # tracked in `schema_migrations`.
    Ecto.Migrator.run(
      Scriba.Test.Repo,
      [{1, Scriba.Test.Migrations.CreateScribaTables}],
      :up,
      all: true
    )

    # Sandbox in :manual mode — tests must explicitly check out a connection.
    #
    # For single-process tests the standard pattern is:
    #
    #     setup do
    #       :ok = Ecto.Adapters.SQL.Sandbox.checkout(Scriba.Test.Repo)
    #     end
    #
    # That is NOT enough for property_db tests in (PD1/PD2/
    # PD3). Those exercise the projection's full process tree — Coordinator,
    # Pipeline, Broadway processors, batchers — and ALL of those processes
    # need to share the same sandboxed connection, which `checkout/1` won't
    # span. will use `Ecto.Adapters.SQL.Sandbox.start_owner!/2`
    # + `allow/3` (or the {:shared, owner_pid} mode) to share the owner's
    # connection across the projection's processes. See Ecto.Adapters.SQL.Sandbox
    # docs §"Allowance and ownership". TODO when lands.
    Ecto.Adapters.SQL.Sandbox.mode(Scriba.Test.Repo, :manual)
end
