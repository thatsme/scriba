defmodule ScribaTest do
  # async: false — the tests start/stop projections that register under
  # `Scriba.Registry` and `Scriba.Projections.Supervisor`, both global.
  # Parallel tests would collide on the same name.
  use ExUnit.Case, async: false

  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test

  # Most tests in this file produce log noise during teardown: Broadway's
  # batch processor can be mid-handle_batch when on_exit's stop_projection
  # kicks off, and the start_supervised Agent cleanup races against
  # Broadway's drain. The noise is cosmetic — the tests assert success
  # before teardown. Capture by default; opt OUT of capture via @tag if a
  # test needs to inspect logs.
  @moduletag :capture_log

  # Module-using-the-macro fixtures. Defined at the top level so they're
  # compiled once and shared across tests. Each test that starts the
  # projection passes a different :agent at runtime via start_projection/2's
  # override mechanism — except this is forbidden by the macro's contract
  # (target carries the agent reference, target is in the config). So
  # each test uses a fresh module via Module.create at test time when
  # an agent ref is needed.

  describe "Scriba.start_projection/1" do
    test "starts a projection from a use Scriba.Projection module" do
      agent = start_supervised!({TestTarget, []})
      name = "start-#{:erlang.unique_integer([:positive])}"

      module = make_projection(name, agent)

      assert {:ok, sup_pid} = Scriba.start_projection(module)
      assert is_pid(sup_pid)

      on_exit(fn -> stop_projection(name, 1) end)

      assert_running(module)
    end

    test "returns {:error, :already_started} on second call" do
      agent = start_supervised!({TestTarget, []})
      name = "already-#{:erlang.unique_integer([:positive])}"

      module = make_projection(name, agent)

      assert {:ok, _} = Scriba.start_projection(module)
      on_exit(fn -> stop_projection(name, 1) end)

      assert_running(module)

      assert {:error, :already_started} = Scriba.start_projection(module)
    end
  end

  describe "Scriba.start_projection/2 — runtime override semantics" do
    test "merges overrides into the module's compile-time config" do
      agent = start_supervised!({TestTarget, []})
      name = "override-#{:erlang.unique_integer([:positive])}"

      module = make_projection(name, agent, parallelism: 2)

      # Override parallelism to 4; the projection should run with the
      # override applied. Verify indirectly by checking the projection
      # comes up successfully under the override.
      assert {:ok, _} = Scriba.start_projection(module, parallelism: 4)
      on_exit(fn -> stop_projection(name, 1) end)

      assert_running(module)
    end

    test "rejects :name override with ArgumentError" do
      module = make_projection("identity-name", start_supervised!({TestTarget, []}))

      assert_raise ArgumentError, ~r/cannot override :name/, fn ->
        Scriba.start_projection(module, name: "different-name")
      end
    end

    test "rejects :version override with ArgumentError" do
      module = make_projection("identity-version", start_supervised!({TestTarget, []}))

      assert_raise ArgumentError, ~r/cannot override :version/, fn ->
        Scriba.start_projection(module, version: 2)
      end
    end
  end

  describe "Scriba.list/0" do
    test "returns running projections as %{name, version, state} maps" do
      agent1 = start_supervised!({TestTarget, []}, id: :tgt1)
      agent2 = start_supervised!({TestTarget, []}, id: :tgt2)
      name1 = "list-a-#{:erlang.unique_integer([:positive])}"
      name2 = "list-b-#{:erlang.unique_integer([:positive])}"

      module1 = make_projection(name1, agent1)
      module2 = make_projection(name2, agent2)

      {:ok, _} = Scriba.start_projection(module1)
      {:ok, _} = Scriba.start_projection(module2)

      on_exit(fn ->
        stop_projection(name1, 1)
        stop_projection(name2, 1)
      end)

      eventually(fn ->
        names = Scriba.list() |> Enum.map(& &1.name) |> Enum.sort()
        assert name1 in names
        assert name2 in names
      end)

      entry1 = Enum.find(Scriba.list(), &(&1.name == name1))
      assert entry1.version == 1
      assert entry1.state in [:initializing, :running]
    end
  end

  describe "Module-form dispatch on pause/resume/stop/info" do
    test "pause/1, resume/1, stop/1, info/1 accept a module reference" do
      agent = start_supervised!({TestTarget, []})
      name = "module-dispatch-#{:erlang.unique_integer([:positive])}"
      module = make_projection(name, agent)

      {:ok, _} = Scriba.start_projection(module)
      on_exit(fn -> stop_projection(name, 1) end)

      assert_running(module)

      assert :ok = Scriba.pause(module)
      assert {:ok, info} = Scriba.info(module)
      assert info.status == :paused

      assert :ok = Scriba.resume(module)
      assert_running(module)

      assert :ok = Scriba.stop(module)
      assert {:ok, info} = Scriba.info(module)
      assert info.status == :stopped
    end

    test "string form Scriba.pause/1 defaults to version 1" do
      agent = start_supervised!({TestTarget, []})
      name = "string-dispatch-#{:erlang.unique_integer([:positive])}"
      module = make_projection(name, agent)

      {:ok, _} = Scriba.start_projection(module)
      on_exit(fn -> stop_projection(name, 1) end)

      assert_running(name)

      assert :ok = Scriba.pause(name)
      assert {:ok, info} = Scriba.info(name)
      assert info.status == :paused
    end

    test "raises clear error when module isn't loaded" do
      # Use a definitely-nonexistent module — atom is created but no
      # corresponding compiled module exists.
      bogus = :"NoSuchProjectionModule_#{:erlang.unique_integer([:positive])}"

      assert_raise ArgumentError, ~r/not loaded/, fn ->
        Scriba.pause(bogus)
      end
    end

    test "raises clear error when module isn't a Scriba projection" do
      # ScribaTest module IS loaded but has no __scriba_config__/0 — wrong
      # module kind.
      assert_raise ArgumentError, ~r/Scriba projection/, fn ->
        Scriba.pause(ScribaTest)
      end
    end
  end

  describe "[:scriba, :projection, :started] telemetry" do
    test "fires exactly once when projection transitions :initializing → :running" do
      ref = make_ref()
      handler_id = {:started_test, ref}
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:scriba, :projection, :started],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {ref, :started, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      agent = start_supervised!({TestTarget, []})
      name = "started-tel-#{:erlang.unique_integer([:positive])}"
      module = make_projection(name, agent)

      {:ok, _} = Scriba.start_projection(module)
      on_exit(fn -> stop_projection(name, 1) end)

      assert_receive {^ref, :started, measurements, metadata}, 2_000
      assert is_integer(measurements.system_time)
      assert metadata.projection == %{name: name, version: 1}

      # Pipeline DOWN → re-up does NOT re-fire :started. Trigger by
      # terminating the Pipeline child and waiting for rest_for_one to
      # respawn it; the Coordinator should re-enter :running via
      # :initializing without firing :started again.
      [{sup_pid, _}] = Registry.lookup(Scriba.Registry, {:projection_supervisor, name, 1})
      :ok = Supervisor.terminate_child(sup_pid, Scriba.Projection.Pipeline)
      {:ok, _} = Supervisor.restart_child(sup_pid, Scriba.Projection.Pipeline)

      assert_running(module)

      refute_receive {^ref, :started, _, _}, 200
    end
  end

  ## Test helpers

  # Defines a module-with-use-Scriba.Projection on the fly with a unique
  # name + the given target Agent. The macro's compile-time config is
  # captured per module, so each test gets its own identity.
  defp make_projection(name, agent, extra_opts \\ []) do
    module_name = String.to_atom("Elixir.ScribaTest.Projection_#{name}")

    opts = [
      name: name,
      source: {Scriba.Test.Source, events: Test.Events.list(3)},
      target: {TestTarget, agent: agent},
      parallelism: Keyword.get(extra_opts, :parallelism, 1),
      handler: Scriba.Test.Projection
    ]

    contents =
      quote do
        use Scriba.Projection, unquote(opts)

        # No custom handle/2 — the :handler opt redirects to
        # Scriba.Test.Projection, which already defines handle/2.
      end

    Module.create(module_name, contents, Macro.Env.location(__ENV__))
    module_name
  end

  defp stop_projection(name, version) do
    # Best-effort teardown: stop if still running, then terminate the
    # per-projection supervisor from the DynamicSupervisor.
    case Scriba.stop(name, version) do
      :ok -> :ok
      _ -> :ok
    end

    case Registry.lookup(Scriba.Registry, {:projection_supervisor, name, version}) do
      [{sup_pid, _}] -> DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, sup_pid)
      [] -> :ok
    end
  end

  # Convenience: asserts that the projection reaches :running.
  defp assert_running(module_or_name) do
    eventually(fn ->
      assert {:ok, info} = Scriba.info(module_or_name)
      assert info.status == :running
    end)
  end

  defp eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, 20)
  end

  defp do_eventually(fun, deadline, interval) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) > deadline do
        reraise e, __STACKTRACE__
      else
        Process.sleep(interval)
        do_eventually(fun, deadline, interval)
      end
  end
end
