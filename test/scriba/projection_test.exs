defmodule Scriba.ProjectionTest do
  use ExUnit.Case, async: true

  describe "use Scriba.Projection — compile-time validation" do
    test "generates __scriba_config__/0 returning a map with declared opts" do
      defmodule HappyProjection do
        use Scriba.Projection,
          name: "happy",
          source: {Scriba.Test.Source, events: []},
          target: {Scriba.Target.Test, agent: :placeholder},
          parallelism: 4

        def handle(_event, _meta), do: :skip
      end

      config = HappyProjection.__scriba_config__()
      assert config.name == "happy"
      assert config.version == 1
      assert config.parallelism == 4
      assert config.partition_by == :stream_id
      assert config.handler == HappyProjection
      assert config.source == {Scriba.Test.Source, [events: []]}
      assert config.target == {Scriba.Target.Test, [agent: :placeholder]}
    end

    test "explicit :version is preserved" do
      defmodule VersionedProjection do
        use Scriba.Projection,
          name: "versioned",
          version: 3,
          source: {Scriba.Test.Source, events: []},
          target: {Scriba.Target.Test, agent: :placeholder},
          parallelism: 1

        def handle(_event, _meta), do: :skip
      end

      assert VersionedProjection.__scriba_config__().version == 3
    end

    test "explicit :handler overrides the default __MODULE__" do
      defmodule WithExplicitHandler do
        use Scriba.Projection,
          name: "explicit-handler",
          source: {Scriba.Test.Source, events: []},
          target: {Scriba.Target.Test, agent: :placeholder},
          parallelism: 1,
          handler: Scriba.Test.Projection

        def handle(_event, _meta), do: :skip
      end

      assert WithExplicitHandler.__scriba_config__().handler == Scriba.Test.Projection
    end

    test "batch_size, batch_timeout, retry pass through when specified" do
      defmodule WithPipelineOpts do
        use Scriba.Projection,
          name: "with-pipeline-opts",
          source: {Scriba.Test.Source, events: []},
          target: {Scriba.Target.Test, agent: :placeholder},
          parallelism: 1,
          batch_size: 25,
          batch_timeout: 200,
          retry: false

        def handle(_event, _meta), do: :skip
      end

      config = WithPipelineOpts.__scriba_config__()
      assert config.batch_size == 25
      assert config.batch_timeout == 200
      assert config.retry == false
    end

    test "batch_size etc. are absent from config when omitted (Pipeline applies its own defaults)" do
      defmodule MinimalOpts do
        use Scriba.Projection,
          name: "minimal-opts",
          source: {Scriba.Test.Source, events: []},
          target: {Scriba.Target.Test, agent: :placeholder},
          parallelism: 1

        def handle(_event, _meta), do: :skip
      end

      config = MinimalOpts.__scriba_config__()
      refute Map.has_key?(config, :batch_size)
      refute Map.has_key?(config, :batch_timeout)
      refute Map.has_key?(config, :retry)
    end

    test "missing :name raises ArgumentError at compile time" do
      assert_raise ArgumentError, ~r/requires :name/, fn ->
        Code.eval_quoted(
          quote do
            defmodule MissingName do
              use Scriba.Projection,
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1
            end
          end
        )
      end
    end

    test "missing :parallelism raises with a guiding message" do
      assert_raise ArgumentError, ~r/requires :parallelism/, fn ->
        Code.eval_quoted(
          quote do
            defmodule MissingParallelism do
              use Scriba.Projection,
                name: "missing-parallelism",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder}
            end
          end
        )
      end
    end

    test "non-:stream_id :partition_by raises, saying custom partitioners are unsupported" do
      # Asserting the limitation rather than the version it was introduced in:
      # the message outlives any particular release, and pinning a version
      # string makes the test fail when the docs are brought up to date.
      assert_raise ArgumentError, ~r/custom partitioners are not supported/, fn ->
        Code.eval_quoted(
          quote do
            defmodule BadPartitionBy do
              use Scriba.Projection,
                name: "bad-partition",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                partition_by: :event_id
            end
          end
        )
      end
    end

    test "unknown option raises with the list of valid options" do
      assert_raise ArgumentError, ~r/Unknown option/, fn ->
        Code.eval_quoted(
          quote do
            defmodule UnknownOpt do
              use Scriba.Projection,
                name: "unknown-opt",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                paralelism: 1
            end
          end
        )
      end
    end

    # Options carried over from a commanded_ecto_projections projector get a
    # targeted error instead of the generic "Unknown option" one. :consistency
    # matters most: it is the only migration gap that is invisible at runtime.
    # The projection works, `dispatch/2` simply stops waiting for it, and the
    # failure surfaces later as a stale read. A generic "unknown option" error
    # invites the reader to delete the line and move on still believing they
    # have the guarantee, so the message has to say the guarantee is gone.
    test "consistency: :strong raises explaining the guarantee is gone" do
      assert_raise ArgumentError, ~r/will NOT wait for this projection/, fn ->
        Code.eval_quoted(
          quote do
            defmodule StrongConsistency do
              use Scriba.Projection,
                name: "strong-consistency",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                consistency: :strong
            end
          end
        )
      end
    end

    test ":application and :repo point at the source/target tuples" do
      assert_raise ArgumentError, ~r/belongs to the source/, fn ->
        Code.eval_quoted(
          quote do
            defmodule LegacyApplication do
              use Scriba.Projection,
                name: "legacy-application",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                application: MyApp.CommandedApp
            end
          end
        )
      end

      assert_raise ArgumentError, ~r/belongs to the target/, fn ->
        Code.eval_quoted(
          quote do
            defmodule LegacyRepo do
              use Scriba.Projection,
                name: "legacy-repo",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                repo: MyApp.Repo
            end
          end
        )
      end
    end

    test ":schema_prefix raises rather than being silently ignored" do
      assert_raise ArgumentError, ~r/does not support :schema_prefix/, fn ->
        Code.eval_quoted(
          quote do
            defmodule LegacySchemaPrefix do
              use Scriba.Projection,
                name: "legacy-schema-prefix",
                source: {Scriba.Test.Source, events: []},
                target: {Scriba.Target.Test, agent: :placeholder},
                parallelism: 1,
                schema_prefix: "tenant_1"
            end
          end
        )
      end
    end

    test ~s(:name matching "_v\\d+" pattern emits a compile-time warning) do
      # IO.warn at compile time goes to stderr. Capture and assert it
      # mentions both the pattern and the doc-aligned shape.
      warning =
        ExUnit.CaptureIO.capture_io(:stderr, fn ->
          Code.eval_quoted(
            quote do
              defmodule LegacyVersionSuffix do
                use Scriba.Projection,
                  name: "orders_v1",
                  source: {Scriba.Test.Source, events: []},
                  target: {Scriba.Target.Test, agent: :placeholder},
                  parallelism: 1

                def handle(_event, _meta), do: :skip
              end
            end
          )
        end)

      assert warning =~ "_v"
      assert warning =~ "version"
      assert warning =~ ~s("orders")
    end
  end
end
