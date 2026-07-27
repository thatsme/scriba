defmodule Scriba.PropertyDb.E3FaultInjectionTest do
  @moduledoc """
  E3 — fault injection against real Postgres.

  Everything about `Scriba.Failure` was previously verified with hand-built
  `%Postgrex.Error{}` structs. That proves the *mapping* — that `"42703"`
  classifies as `:structural` — and proves nothing about whether Postgres
  emits `42703` through this code path, or whether the error arrives with
  `:postgres` populated the way the pattern expects.

  These tests induce the real failures and assert the whole path: Postgres
  error → target → classifier → pipeline response → dead-letter row or halt.

  The conservation identity is checked throughout:

      events in == read-model rows + dead letters + skipped

  Any shortfall is an event that vanished.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Ecto.Adapters.SQL
  alias Scriba.Projection.Supervisor, as: ProjSup
  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.ReadModel
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  defp read_model_count(stream_id) do
    %{rows: [[n]]} =
      SQL.query!(Repo, "SELECT count(*) FROM test_read_models WHERE stream_id = $1", [stream_id])

    n
  end

  defp dead_letters(name) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        "SELECT position, error_kind, error_message FROM scriba_dead_letters WHERE projection_name = $1 ORDER BY position",
        [name]
      )

    rows
  end

  defp cursor(name) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        "SELECT stream_id, position FROM scriba_positions WHERE projection_name = $1",
        [name]
      )

    Map.new(rows, fn [sid, pos] -> {sid, pos} end)
  end

  @doc false
  def forward(event, measurements, metadata, %{test_pid: pid, ref: ref}) do
    send(pid, {ref, event, measurements, metadata})
  end

  defp attach(ref, events) do
    id = {__MODULE__, ref}
    :telemetry.attach_many(id, events, &__MODULE__.forward/4, %{test_pid: self(), ref: ref})
    on_exit(fn -> :telemetry.detach(id) end)
  end

  # ── Integrity ─────────────────────────────────────────────────────────────

  defmodule DuplicateKeyHandler do
    @moduledoc false
    # Event at position 2 inserts a primary key that already exists, so
    # Postgres rejects it with 23505 while its batch-mates are fine.
    def handle(_data, %{position: 2, stream_id: sid}) do
      {:insert, %ReadModel{event_id: "e3-preexisting", stream_id: sid, position: 2}}
    end

    def handle(_data, meta) do
      {:insert, %ReadModel{event_id: meta.id, stream_id: meta.stream_id, position: meta.position}}
    end
  end

  test "integrity: a duplicate key dead-letters one event and the batch continues" do
    ref = make_ref()
    attach(ref, [[:scriba, :projection, :dead_letter]])

    name = "e3-integrity-#{:erlang.unique_integer([:positive])}"
    events = Scriba.Test.Events.list(3, streams: 1)
    [%{stream_id: sid} | _] = events

    # The row event 2 will collide with.
    SQL.query!(
      Repo,
      "INSERT INTO test_read_models (event_id, stream_id, position) VALUES ($1, $2, $3)",
      ["e3-preexisting", sid, 0]
    )

    opts = [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {Scriba.Target.Ecto, repo: Repo},
      parallelism: 1,
      handler: DuplicateKeyHandler,
      batch_size: 10,
      batch_timeout: 50,
      retry: false
    ]

    ExUnit.CaptureLog.capture_log(fn ->
      start_supervised!({ProjSup, opts})
      assert_receive {^ref, [:scriba, :projection, :dead_letter], _m, meta}, 10_000
      assert meta.position == 2
      # Recorded for the report: this is the label Postgres+Ecto actually
      # produce, which is the thing hand-built structs could not tell us.
      IO.puts("\n[E3 integrity] error_kind = #{inspect(meta.error_kind)}")
    end)

    Process.sleep(300)

    [[pos, kind, _msg]] = dead_letters(name)
    assert pos == 2
    assert kind =~ "commit:"

    # Events 1 and 3 committed; the pre-existing row is still there.
    assert read_model_count(sid) == 3

    # Conservation: 3 events in == 2 committed + 1 dead-lettered + 0 skipped.
    assert read_model_count(sid) - 1 + length(dead_letters(name)) == 3

    # Cursor advanced past all three — not wedged on the poison event.
    assert cursor(name) == %{sid => 3}
  end

  # ── Structural ────────────────────────────────────────────────────────────

  defmodule UndefinedColumnHandler do
    @moduledoc false
    # Raw SQL against a column that does not exist — the "handler deployed
    # ahead of its migration" shape, guaranteed to produce SQLSTATE 42703.
    def handle(_data, _meta) do
      {:multi,
       Ecto.Multi.run(Ecto.Multi.new(), :bad_column, fn repo, _changes ->
         SQL.query(repo, "UPDATE test_read_models SET no_such_column = 1", [])
       end)}
    end
  end

  test "structural: an undefined column halts the projection, discarding nothing" do
    ref = make_ref()
    attach(ref, [[:scriba, :projection, :halted], [:scriba, :projection, :dead_letter]])

    name = "e3-structural-#{:erlang.unique_integer([:positive])}"
    events = Scriba.Test.Events.list(3, streams: 1)
    [%{stream_id: sid} | _] = events

    opts = [
      name: name,
      version: 1,
      source: {Scriba.Test.Source, events: events},
      target: {Scriba.Target.Ecto, repo: Repo},
      parallelism: 1,
      handler: UndefinedColumnHandler,
      batch_size: 10,
      batch_timeout: 50,
      retry: false
    ]

    ExUnit.CaptureLog.capture_log(fn ->
      start_supervised!({ProjSup, opts})

      assert_receive {^ref, [:scriba, :projection, :halted], _m, meta}, 10_000
      IO.puts("\n[E3 structural] failure = #{inspect(meta.failure)}")

      # The SQLSTATE survived Ecto and reached the classifier intact. If this
      # fails, classification was running on something other than the code it
      # was written against.
      assert meta.failure =~ "42703"
    end)

    Process.sleep(300)

    # Nothing discarded and nothing advanced: no dead letters, no cursor, no
    # read-model rows. The events are still owed.
    assert dead_letters(name) == []
    assert cursor(name) == %{}
    assert read_model_count(sid) == 0

    # Does an operator who missed the telemetry have any way to see this?
    {:ok, info} = Scriba.info(name, 1)
    IO.puts("[E3 structural] Scriba.info/2 status = #{inspect(info.status)}")
  end
end
