defmodule Scriba.PropertyDb.SandboxHarnessTest do
  @moduledoc """
  Foundation for PD1/PD2/PD3.

  Asserts that the shared-mode `Ecto.Adapters.SQL.Sandbox` setup spans the
  entire projection process tree: Coordinator, Pipeline (Broadway top
  supervisor), Broadway producer (the Test source), processors, batcher,
  and the Ecto target's transaction-running process.

  How the test pins this down: it starts a real projection writing to
  `Scriba.Test.Repo` via `Scriba.Target.Ecto`, runs three events through
  it, then queries `scriba_positions` from the test process. If sandbox
  sharing isn't working, the projection committed via a different
  connection and the test's query sees nothing.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  test "Coordinator + Pipeline + Broadway tree share the test's sandboxed connection" do
    name = "harness-#{:erlang.unique_integer([:positive])}"
    events = Scriba.Test.Events.list(3, streams: 1)

    opts = [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {Scriba.Target.Ecto, repo: Repo},
      parallelism: 1,
      handler: __MODULE__.NoopMultiHandler,
      batch_size: 3,
      batch_timeout: 50
    ]

    # Telemetry-driven completion signal. [:broadway, :batch_processor, :stop]
    # fires after Broadway's batch_processor stage finishes — i.e. after our
    # handle_batch returns, after target.apply_batch's Repo.transaction has
    # committed. This is the only deterministic point at which the
    # scriba_positions row is guaranteed to be visible.
    ref = make_ref()
    handler_id = {:harness, ref}

    :telemetry.attach(
      handler_id,
      [:broadway, :batch_processor, :stop],
      &__MODULE__.forward_batch_done/4,
      %{test_pid: self(), ref: ref}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    start_supervised!({ProjSup, opts})

    # Single batch (batch_size: 3, three events) — wait for one batch_done.
    assert_receive {^ref, :batch_done}, 5_000

    # If shared sandbox is working, the projection committed its writes
    # through this test's connection and they're visible from this query.
    # If it's not, the projection ran on a different connection inside its
    # own transaction, and the query below returns an empty result.
    %{rows: rows} =
      Ecto.Adapters.SQL.query!(
        Repo,
        """
        SELECT stream_id, position
          FROM scriba_positions
         WHERE projection_name = $1
           AND projection_version = $2
         ORDER BY stream_id
        """,
        [name, 1]
      )

    assert rows == [["stream-0", 3]],
           "Expected scriba_positions[stream-0] = 3 for projection #{name}; got #{inspect(rows)}. " <>
             "Empty result usually means the projection's process tree wrote to a different " <>
             "connection — sandbox sharing isn't reaching Broadway-spawned processes."
  end

  @doc false
  def forward_batch_done(_event, _measurements, _metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, :batch_done})
  end

  defmodule NoopMultiHandler do
    @moduledoc """
    Returns `{:multi, Ecto.Multi.new()}` — a non-`:skip` handler result
    that adds nothing to the read-model write side but still triggers
    `Position.multi/5` to upsert `scriba_positions`. That's the only
    side effect this harness test needs.
    """
    def handle(_event, _meta), do: {:multi, Ecto.Multi.new()}
  end
end
