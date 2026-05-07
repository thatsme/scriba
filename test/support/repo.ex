defmodule Scriba.Test.Repo do
  @moduledoc """
  Test-only `Ecto.Repo` for real-Postgres property tests
  (`test/property_db/`, ).

  Configured by `config/test.exs` from `SCRIBA_TEST_DB_*` environment
  variables. When the env is not configured, this module is still compiled
  (it's `use Ecto.Repo` after all), but `Scriba.Test.Repo.start_link/0`
  will fail because no config is present — `test_helper.exs` checks for
  the config before calling `start_link/0` and excludes property_db tests
  cleanly when missing.

  Pool is `Ecto.Adapters.SQL.Sandbox` (in `:manual` mode) so each test
  checks out its own connection in `setup` for transactional isolation.
  """

  use Ecto.Repo,
    otp_app: :scriba,
    adapter: Ecto.Adapters.Postgres
end
