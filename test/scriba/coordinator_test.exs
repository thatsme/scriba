defmodule Scriba.Projection.CoordinatorTest do
  use ExUnit.Case, async: true

  alias Scriba.Projection.Coordinator
  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test

  setup do
    # Start Agent first → ProjSup second. ExUnit teardown stops in reverse,
    # so ProjSup (and its Pipeline's final batch) finishes before the Agent
    # is taken down — no "no process" log spam during teardown.
    agent = start_supervised!({TestTarget, []})
    events = Test.Events.list(10, streams: 3)
    name = "coord-#{:erlang.unique_integer([:positive])}"

    opts = [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {TestTarget, agent: agent},
      parallelism: 2,
      handler: Scriba.Test.Projection,
      batch_size: 5,
      batch_timeout: 50
    ]

    start_supervised!({ProjSup, opts})

    %{name: name, version: 1, agent: agent}
  end

  describe "state transitions" do
    test ":idle auto-transitions to :running", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
    end

    test ":running → :paused on pause", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused
    end

    test ":paused → :running on resume", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert Coordinator.state(name, v) == :paused

      assert :ok = Coordinator.resume(name, v)
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
    end

    test ":running → :stopped on stop (passes through :draining)", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped
    end

    test ":paused → :stopped on stop", %{name: name, version: v} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert :ok = Coordinator.stop(name, v)
      assert Coordinator.state(name, v) == :stopped
    end
  end

  describe "invalid command/state combinations" do
    test "pause from :paused returns {:error, {:invalid_state, :paused}}", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      :ok = Coordinator.pause(name, v)
      assert {:error, {:invalid_state, :paused}} = Coordinator.pause(name, v)
    end

    test "resume from :running returns {:error, {:invalid_state, :running}}", %{
      name: name,
      version: v
    } do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      assert {:error, {:invalid_state, :running}} = Coordinator.resume(name, v)
    end
  end

  describe "end-to-end with Test source and Test target" do
    test "all events flow source → handler → target", %{name: name, version: v, agent: agent} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      assert TestTarget.safe_position(agent) >= 1
    end

    test "preserves per-stream ordering", %{name: name, version: v, agent: agent} do
      eventually(fn -> assert Coordinator.state(name, v) == :running end)
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      commits = TestTarget.commits(agent)

      # commits is now [{event_id, stream_id, position}, ...] in commit order;
      # group by the stream_id field directly and assert per-stream order.
      by_stream = Enum.group_by(commits, fn {_id, sid, _pos} -> sid end)

      for {stream_id, stream_commits} <- by_stream do
        positions = Enum.map(stream_commits, fn {_id, _sid, p} -> p end)

        assert positions == Enum.sort(positions),
               "per-stream ordering violated for #{stream_id}: #{inspect(positions)}"
      end
    end
  end

  defp eventually(fun, timeout \\ 1_000) do
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
