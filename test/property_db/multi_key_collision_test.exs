defmodule Scriba.PropertyDb.MultiKeyCollisionTest do
  @moduledoc """
  Regression test for the batch-vs-per-event `Ecto.Multi` keying hazard.

  `commanded_ecto_projections` ran one `Repo.transaction/1` per event, so a
  projector could name its Multi operation with a static atom
  (`:example_projection`) forever without conflict — and the library's own
  docs used exactly that shape.

  Scriba merges an entire batch into ONE `Ecto.Multi`. A static key therefore
  collides as soon as two events land in the same batch, and `Ecto.Multi`
  raises on duplicate operation names.

  This test lives in the real-Postgres suite because Ecto resolves
  `Ecto.Multi.merge/2` lazily: `Ecto.Multi.to_list/1` reports an unresolved
  `:merge` entry, so the collision is invisible until a transaction actually
  runs. Asserting it therefore requires a real `Repo.transaction/1` — the
  alternative would be reaching into Ecto's private apply path, which is not
  a dependency this library should take.

  ## What this pins, now that the Pipeline guards it

  `Scriba.Projection.Pipeline` detects duplicate operation names across a
  batch and dead-letters the later claimant, so in normal operation this
  collision no longer reaches a transaction. These tests call
  `Scriba.Target.Ecto.build_multi/5` directly, bypassing that guard, and
  therefore pin the underlying Ecto behaviour the guard exists to prevent.

  If the second test ever stops raising — because Ecto started tolerating
  duplicate names, say — the Pipeline guard has become unnecessary, and the
  cost it imposes (a `to_list/1` scan per `{:multi, _}` result per batch) is
  no longer buying anything.

  The DB-free halves of the contract are in the default suite:
  `test/scriba/target/ecto_test.exs` for Scriba's own steps keying on event
  id, and `test/scriba/projection/pipeline_test.exs` for the dead-lettering
  behaviour itself.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Event
  alias Scriba.Target.Ecto, as: EctoTarget
  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  defp event(id, position, stream_id) do
    %Event{
      id: id,
      stream_id: stream_id,
      type: "TestEvent",
      data: %{},
      position: position,
      occurred_at: DateTime.utc_now(),
      metadata: %{}
    }
  end

  defp multi_result(key) do
    {:multi, Ecto.Multi.run(Ecto.Multi.new(), key, fn _repo, _changes -> {:ok, key} end)}
  end

  defp two_event_batch(name, results) do
    events = [event("e1", 1, "stream-a"), event("e2", 2, "stream-a")]
    advances = %{"stream-a" => 2}

    EctoTarget.build_multi(events, results, %{name: name, version: 1}, advances, [])
  end

  test "event-unique Multi keys commit — the remedy MIGRATION.md prescribes" do
    name = "collision-ok-#{:erlang.unique_integer([:positive])}"
    multi = two_event_batch(name, [multi_result({:my_op, "e1"}), multi_result({:my_op, "e2"})])

    assert {:ok, changes} = Repo.transaction(multi)
    assert changes[{:my_op, "e1"}] == {:my_op, "e1"}
    assert changes[{:my_op, "e2"}] == {:my_op, "e2"}
  end

  test "a static Multi key reused across two events in one batch collides" do
    name = "collision-bad-#{:erlang.unique_integer([:positive])}"

    multi =
      two_event_batch(name, [
        multi_result(:example_projection),
        multi_result(:example_projection)
      ])

    # RuntimeError, not ArgumentError. Ecto.Multi.append/2 raises ArgumentError
    # on duplicate names, but the *merge* path — which is what Scriba uses for
    # `{:multi, _}` — raises RuntimeError from Ecto.Multi.merge_results/3.
    # Asserting ArgumentError here was an inference carried over from a
    # DB-free probe of append/2; the first real-Postgres run corrected it.
    assert_raise RuntimeError, ~r/example_projection/, fn ->
      Repo.transaction(multi)
    end
  end
end
