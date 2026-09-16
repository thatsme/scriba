defmodule Scriba.PropertyDb.LagTelemetryTest do
  @moduledoc """
  `[:scriba, :projection, :lag]`.

  The event reads the watermark from Postgres, so it needs a real database.
  What is pinned here is the contract an alert would be written against: it
  fires on a timer rather than on traffic, it carries the lag in
  milliseconds and the watermark it came from, and it says nothing at all
  until the projection has committed something — a projection with no
  watermark reporting `lag_ms: 0` would read as caught-up when it has not
  started.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo
  alias Scriba.Watermark

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  setup do
    name = "lag-#{System.unique_integer([:positive])}"
    ref = make_ref()
    handler_id = {:lag_test, ref}

    :telemetry.attach(
      handler_id,
      [:scriba, :projection, :lag],
      &__MODULE__.forward/4,
      %{test_pid: self(), ref: ref, name: name}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    %{name: name, ref: ref}
  end

  @doc false
  def forward(_event, measurements, metadata, %{test_pid: pid, ref: ref, name: name}) do
    # Telemetry handlers are global, so other projections' ticks arrive here
    # too. Filter on the name this test owns.
    if metadata.projection.name == name do
      send(pid, {ref, measurements, metadata})
    end
  end

  test "reports lag measured from the event's own timestamp", %{name: name, ref: ref} do
    projection = %{name: name, version: 1}
    :ok = Watermark.put(Repo, projection, 71, DateTime.add(DateTime.utc_now(), -8, :second))

    start_coordinator(name, lag_interval: 100)

    assert_receive {^ref, measurements, metadata}, 2_000

    assert measurements.watermark == 71
    assert measurements.lag_ms >= 8_000
    assert measurements.lag_ms < 20_000
    assert metadata.projection == projection
    assert metadata.status in [:initializing, :running]
  end

  test "keeps reporting on a timer, with no events flowing", %{name: name, ref: ref} do
    :ok = Watermark.put(Repo, %{name: name, version: 1}, 5, DateTime.utc_now())

    start_coordinator(name, lag_interval: 100)

    assert_receive {^ref, _, _}, 2_000
    assert_receive {^ref, _, _}, 2_000

    # An idle projection is exactly when lag matters: nothing is arriving to
    # drive an event-triggered metric, and the number still has to move.
  end

  test "says nothing until the projection has committed something", %{name: name, ref: ref} do
    start_coordinator(name, lag_interval: 100)

    refute_receive {^ref, _, _}, 1_000
  end

  test "can be turned off", %{name: name, ref: ref} do
    :ok = Watermark.put(Repo, %{name: name, version: 1}, 5, DateTime.utc_now())

    start_coordinator(name, lag_interval: 0)

    refute_receive {^ref, _, _}, 1_000
  end

  defp start_coordinator(name, opts) do
    # The Coordinator alone, without a Pipeline: lag reporting is its job and
    # it must not depend on a source being reachable. It stays in
    # :initializing here, polling for a producer that never arrives, which is
    # itself worth covering — a projection that cannot start is one whose lag
    # an operator very much wants to see growing.
    opts =
      Keyword.merge(
        [
          name: name,
          version: 1,
          source: {Scriba.Test.Source, []},
          target: {Scriba.Target.Ecto, repo: Repo},
          parallelism: 1,
          handler: Scriba.Test.Projection,
          supervisor_pid: self()
        ],
        opts
      )

    pid = start_supervised!({Scriba.Projection.Coordinator, opts})
    Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), pid)
    pid
  end
end
