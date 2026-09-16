defmodule Scriba.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/thatsme/scriba"

  def project do
    [
      app: :scriba,
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      name: "Scriba",
      source_url: @source_url,
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Scriba.Application, []}
    ]
  end

  def cli do
    [
      preferred_envs: [
        "test.fast": :test,
        "test.property_db": :test,
        "test.all": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Required. Broadway is the pipeline runtime; Ecto/Postgrex back the
      # position cursor and dead-letter tables (Scriba.Position,
      # Scriba.DeadLetter, Scriba.Migrations), not just Scriba.Target.Ecto.
      # Marking ecto_sql/postgrex `optional: true` makes Scriba fail to
      # compile for any consumer that omits them — verified, not assumed.
      {:broadway, "~> 1.1"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.17"},
      {:telemetry, "~> 1.2"},
      {:jason, "~> 1.4"},

      # Optional: only needed by Scriba.Source.Commanded. Every reference to
      # Commanded in lib/ is dynamic (apply/3 + atoms built from string
      # literals), so Scriba compiles cleanly without it.
      {:commanded, "~> 1.4", optional: true},

      # dev/test only
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
    ]
  end

  defp description do
    "A projection engine for Elixir event-sourced systems."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/v#{@version}/CHANGELOG.md",
        "Migration guide" => "#{@source_url}/blob/v#{@version}/MIGRATION.md"
      },
      files:
        ~w(lib mix.exs README.md MIGRATION.md REBUILDING.md SCRIBA_ARCHITECTURE.md CHANGELOG.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      # Every .md referenced by a relative link in README.md must be listed
      # here — ex_doc only rewrites relative links that resolve to a known
      # extra. Anything not listed (e.g. examples/, which is excluded from
      # the package tarball) must be linked with an absolute GitHub URL.
      extras: [
        "README.md",
        "MIGRATION.md",
        "REBUILDING.md",
        "SCRIBA_ARCHITECTURE.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: ["MIGRATION.md", "REBUILDING.md", "SCRIBA_ARCHITECTURE.md"]
      ],
      # Internal modules carry @moduledoc false, so ExDoc cannot link to them
      # and warns on every mention. They are named on purpose — the
      # architecture guide, the telemetry catalog and the CHANGELOG all have
      # to say which process emits what — so the mentions stay and the
      # autolinking is skipped instead. Rendered as plain code, no warning.
      #
      # A name only belongs here while the module is hidden: promoting one to
      # public documentation means removing its entry, or the reference
      # silently stops linking.
      skip_code_autolink_to: [
        "Scriba.Registry",
        "Scriba.Supervisor.init/1",
        "Scriba.Projections.Supervisor",
        "Scriba.Projection.Supervisor",
        "Scriba.Projection.Coordinator",
        "Scriba.Projection.Pipeline",
        "Scriba.Projection.Pipeline.handle_batch/4",
        "Scriba.Source.Commanded.start_link/1",
        "Scriba.Source.Commanded.pause/1",
        "Scriba.Source.Commanded.to_message/2",
        "Scriba.Position.safe_position/2"
      ],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  defp aliases do
    [
      # Fast suite — strictly DB-free, regardless of whether SCRIBA_TEST_DB_*
      # is configured. Excludes both `:property` (auto-tagged on
      # ExUnitProperties' property tests) and `:property_db` (real-Postgres
      # tests in test/property_db/). Use this for normal development.
      "test.fast": ["test --exclude property --exclude property_db"],

      # Real-Postgres property tests only. Function alias rather than string
      # alias because we need to gate on SCRIBA_TEST_DB_* env vars BEFORE
      # invoking ExUnit. A naive `test --only property_db` doesn't work:
      # ExUnit's `--only` replaces (rather than merges with) the
      # `exclude: [property_db: true]` that test_helper.exs sets when env
      # is unconfigured, so property_db tests would run and fail with
      # connection errors instead of skipping cleanly. Verified empirically.
      "test.property_db": &__MODULE__.test_property_db/1,

      # Everything ExUnit will run with the current environment. Includes
      # the surviving P1 ordering property; with SCRIBA_TEST_DB_* set, also
      # includes property_db tests. Without those env vars, property_db is
      # excluded by test_helper.exs and this alias still passes cleanly.
      "test.all": ["test"]
    ]
  end

  @scriba_test_db_vars ~w(
    SCRIBA_TEST_DB_HOST
    SCRIBA_TEST_DB_PORT
    SCRIBA_TEST_DB_NAME
    SCRIBA_TEST_DB_USER
    SCRIBA_TEST_DB_PASS
  )

  @doc false
  def test_property_db(args) do
    if Enum.all?(@scriba_test_db_vars, fn v -> System.get_env(v) not in [nil, ""] end) do
      Mix.Task.run("test", ["--only", "property_db"] ++ args)
    else
      Mix.shell().info("""

      mix test.property_db skipped: SCRIBA_TEST_DB_* env vars are not set.

      Set all of: #{Enum.join(@scriba_test_db_vars, ", ")}

      The fast suite (mix test.fast) runs without any of this.
      """)

      :ok
    end
  end
end
