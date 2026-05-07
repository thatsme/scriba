defmodule Scriba.Projection.PipelineTest do
  # async: false — uses :erlang.trace_pattern, which is BEAM-global. Running
  # this concurrently with other tests that touch the same MFA pattern would
  # race on the call_count counter.
  use ExUnit.Case, async: false

  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Target.Test, as: TestTarget
  alias Scriba.Test

  describe "partitioner integration" do
    @tag :integration
    test "Scriba.Partitioner.partition/2 is on the Pipeline's runtime path" do
      # Arm BEAM call-count tracing on the partitioner. This is a structural
      # guarantee against the dead-code finding from the diagnostic phase
      # (Pipeline used to bypass this module via inline `:erlang.phash2`).
      # If anyone ever reverts the wiring in pipeline.ex, this test fails
      # loudly instead of the partitioner module silently going dead.
      :erlang.trace_pattern({Scriba.Partitioner, :partition, 2}, true, [:call_count])

      on_exit(fn ->
        :erlang.trace_pattern({Scriba.Partitioner, :partition, 2}, false, [:call_count])
      end)

      agent = start_supervised!({TestTarget, []})
      events = Test.Events.list(10, streams: 3)
      name = "pipeline-partitioner-#{:erlang.unique_integer([:positive])}"

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

      # Wait for events to flow through.
      eventually(fn -> assert length(TestTarget.commits(agent)) == 10 end, 2_000)

      {:call_count, count} =
        :erlang.trace_info({Scriba.Partitioner, :partition, 2}, :call_count)

      assert count >= 10,
             "Scriba.Partitioner.partition/2 was called #{count} times — expected " <>
               "at least 10 (one per event entering Broadway's partition_by). " <>
               "Zero typically means pipeline.ex has reverted to inline " <>
               ":erlang.phash2 and the partitioner module is dead code again."
    end
  end

  defp eventually(fun, timeout) do
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
