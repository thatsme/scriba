defmodule Scriba.Target.EctoTest do
  use ExUnit.Case, async: true

  alias Scriba.Event
  alias Scriba.Target.Ecto, as: EctoTarget
  alias Scriba.Test.ReadModel

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

  defp keys(multi), do: multi |> Ecto.Multi.to_list() |> Enum.map(&elem(&1, 0))

  defp projection, do: %{name: "p", version: 1}

  defp advances_for(events) do
    events
    |> Enum.group_by(& &1.stream_id)
    |> Map.new(fn {sid, evts} -> {sid, evts |> Enum.map(& &1.position) |> Enum.max()} end)
  end

  describe "build_multi/5 — handler return shapes (§4.2)" do
    test ":skip yields no event op, only the per-stream position update" do
      events = [event("e1", 1, "stream-a")]
      multi = EctoTarget.build_multi(events, [:skip], projection(), advances_for(events), [])

      assert keys(multi) == [{:scriba_position, "stream-a"}]
    end

    test "{:insert, struct} adds an insert step keyed by event id" do
      events = [event("e1", 1, "stream-a")]
      result = {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events), [])

      assert {:scriba_event, "e1"} in keys(multi)
      assert {:scriba_position, "stream-a"} in keys(multi)
    end

    test "{:update, schema, filter, [set: changes]} adds an update_all step" do
      events = [event("e1", 1, "stream-a")]
      result = {:update, ReadModel, [event_id: "e1"], set: [position: 99]}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events), [])

      assert {:scriba_event, "e1"} in keys(multi)
    end

    test "{:delete, schema, filter} adds a delete_all step" do
      events = [event("e1", 1, "stream-a")]
      result = {:delete, ReadModel, [event_id: "e1"]}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events), [])

      assert {:scriba_event, "e1"} in keys(multi)
    end

    test "{:multi, user_multi} merges the user's Multi" do
      events = [event("e1", 1, "stream-a")]
      user_multi = Ecto.Multi.new() |> Ecto.Multi.run(:user_op, fn _, _ -> {:ok, :done} end)
      result = {:multi, user_multi}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events), [])

      ks = keys(multi)
      assert {:scriba_position, "stream-a"} in ks
      assert :merge in ks
    end

    # Regression guard for the batch-vs-per-event Multi keying.
    #
    # commanded_ecto_projections ran one transaction per event, so projectors
    # conventionally used a static Multi key (`:example_projection`). Scriba
    # merges a whole batch into ONE Multi, so any static key collides the
    # moment two events land in the same batch. Scriba's own steps avoid this
    # by keying on event id — this test locks that in.
    #
    # It only fires when a batch holds two events of the same type hitting the
    # same projection, which a one-event-per-test suite never produces. That
    # is exactly why it is written down: dormant in CI, lethal under replay
    # load, where batches are full rather than singletons.
    test "two same-type events in one batch assemble with distinct keys" do
      events = [
        event("e1", 1, "stream-a"),
        event("e2", 2, "stream-a")
      ]

      results = [
        {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}},
        {:insert, %ReadModel{event_id: "e2", stream_id: "stream-a", position: 2}}
      ]

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events), [])
      ks = keys(multi)

      # Assembles at all — Ecto raises on duplicate operation names.
      assert length(ks) == length(Enum.uniq(ks))

      # Both events present under their own key, one shared cursor advance.
      assert {:scriba_event, "e1"} in ks
      assert {:scriba_event, "e2"} in ks
      assert Enum.count(ks, &match?({:scriba_position, "stream-a"}, &1)) == 1
    end

    test "mixed batch — insert + skip + delete on same stream" do
      events = [
        event("e1", 1, "stream-a"),
        event("e2", 2, "stream-a"),
        event("e3", 3, "stream-a")
      ]

      results = [
        {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}},
        :skip,
        {:delete, ReadModel, [event_id: "e1"]}
      ]

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events), [])
      ks = keys(multi)

      assert {:scriba_event, "e1"} in ks
      refute {:scriba_event, "e2"} in ks
      assert {:scriba_event, "e3"} in ks
      assert {:scriba_position, "stream-a"} in ks
    end
  end

  describe "build_multi/5 — {:multi, _} across a multi-event batch" do
    defp multi_result(key) do
      {:multi, Ecto.Multi.run(Ecto.Multi.new(), key, fn _repo, _changes -> {:ok, key} end)}
    end

    # Each `{:multi, _}` is merged independently, so a batch of N such events
    # produces N merge steps against one Multi. That is the structural reason
    # a Multi key reused across two events in the same batch collides at
    # transaction time (Ecto raises on duplicate operation names), where under
    # commanded_ecto_projections' one-transaction-per-event model it could not.
    #
    # Ecto resolves merges lazily, so the collision itself surfaces only inside
    # Repo.transaction/1 — asserted end-to-end in the real-Postgres suite. What
    # is Scriba's to guarantee, and what is checked here, is that the batch
    # carries one independent merge per event rather than flattening them.
    test "each event's Multi is merged independently" do
      events = [event("e1", 1, "stream-a"), event("e2", 2, "stream-a")]
      results = [multi_result({:my_op, "e1"}), multi_result({:my_op, "e2"})]

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events), [])
      ks = keys(multi)

      assert Enum.count(ks, &(&1 == :merge)) == 2
      assert {:scriba_position, "stream-a"} in ks
    end
  end

  describe "build_multi/5 — dead-letter routing" do
    test "{:error, _} dead-letter produces a :scriba_dead_letter step, NOT a failing transaction step" do
      # Regression: the previous build_multi/4 inserted an
      # Ecto.Multi.run/3 step that returned {:error, _}, poisoning the
      # whole transaction. The Pipeline now partitions {:error, _} into
      # the dead_letters argument; this assertion pins that the resulting
      # Multi carries a dead-letter insert keyed `{:scriba_dead_letter, _}`
      # and NOT a `{:scriba_event, _}` failing step.
      e = event("e1", 1, "stream-a")
      dead_letters = [{e, {:error, :boom}}]

      multi = EctoTarget.build_multi([], [], projection(), advances_for([e]), dead_letters)
      ks = keys(multi)

      assert {:scriba_dead_letter, "e1"} in ks
      refute {:scriba_event, "e1"} in ks
      assert {:scriba_position, "stream-a"} in ks
    end

    test "{:exception, exception, stacktrace} produces a :scriba_dead_letter step" do
      e = event("e1", 1, "stream-a")
      exception = %RuntimeError{message: "boom"}
      dead_letters = [{e, {:exception, exception, []}}]

      multi = EctoTarget.build_multi([], [], projection(), advances_for([e]), dead_letters)

      assert {:scriba_dead_letter, "e1"} in keys(multi)
    end

    test "mixed batch — successes go to :scriba_event, failures go to :scriba_dead_letter, cursor advances past both" do
      good = event("e1", 1, "stream-a")
      bad = event("e2", 2, "stream-a")
      good_result = {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}}

      events = [good]
      handler_results = [good_result]
      dead_letters = [{bad, {:error, :nope}}]

      # stream_advances reflects max position across BOTH good AND bad
      # for stream-a — i.e. 2 (the dead-lettered event), not 1.
      advances = %{"stream-a" => 2}

      multi = EctoTarget.build_multi(events, handler_results, projection(), advances, dead_letters)
      ks = keys(multi)

      assert {:scriba_event, "e1"} in ks
      assert {:scriba_dead_letter, "e2"} in ks
      assert {:scriba_position, "stream-a"} in ks
    end
  end

  describe "build_multi/5 — multi-stream batches" do
    test "appends one position step per distinct stream in the batch" do
      events = [
        event("e1", 1, "stream-a"),
        event("e2", 2, "stream-b"),
        event("e3", 3, "stream-a")
      ]

      results = Enum.map(events, fn _ -> :skip end)
      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events), [])
      ks = keys(multi)

      assert {:scriba_position, "stream-a"} in ks
      assert {:scriba_position, "stream-b"} in ks
    end

    test "per-stream position step uses the max position seen in the batch" do
      events = [
        event("e1", 1, "stream-a"),
        event("e2", 5, "stream-a"),
        event("e3", 3, "stream-a")
      ]

      results = Enum.map(events, fn _ -> :skip end)
      advances = advances_for(events)

      assert advances == %{"stream-a" => 5}

      multi = EctoTarget.build_multi(events, results, projection(), advances, [])
      assert {:scriba_position, "stream-a"} in keys(multi)
    end
  end

  describe "build_multi/5 — position updates appended after handler ops" do
    test "all per-stream position steps come after handler steps" do
      events = [event("e1", 1, "stream-a"), event("e2", 2, "stream-b")]
      results = [
        {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}},
        {:insert, %ReadModel{event_id: "e2", stream_id: "stream-b", position: 2}}
      ]

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events), [])
      ks = keys(multi)

      handler_keys = [{:scriba_event, "e1"}, {:scriba_event, "e2"}]
      position_keys = [{:scriba_position, "stream-a"}, {:scriba_position, "stream-b"}]

      handler_max_idx = handler_keys |> Enum.map(&Enum.find_index(ks, fn k -> k == &1 end)) |> Enum.max()
      position_min_idx = position_keys |> Enum.map(&Enum.find_index(ks, fn k -> k == &1 end)) |> Enum.min()

      assert position_min_idx > handler_max_idx
    end
  end

  describe "valid_result?/1" do
    # Guards the crash-loop path: a result this rejects is dead-lettered by the
    # Pipeline before it can reach build_multi/5, where an unmatched shape
    # raises during Multi assembly — outside apply_batch/6's
    # `case repo.transaction(...)` — failing the whole batch on every
    # redelivery. Must stay in lockstep with apply_handler_result/3's clauses.
    test "accepts exactly the success-shape half of §4.2" do
      assert EctoTarget.valid_result?(:skip)
      assert EctoTarget.valid_result?({:insert, %ReadModel{}})
      assert EctoTarget.valid_result?({:update, ReadModel, [event_id: "e1"], set: [position: 1]})
      assert EctoTarget.valid_result?({:delete, ReadModel, [event_id: "e1"]})
      assert EctoTarget.valid_result?({:multi, Ecto.Multi.new()})
    end

    test "rejects near-misses and malformed shapes" do
      # Typo'd tag.
      refute EctoTarget.valid_result?({:updat, ReadModel, [id: 1], set: [x: 2]})
      # :update without the [set: _] keyword.
      refute EctoTarget.valid_result?({:update, ReadModel, [id: 1], [inc: [x: 2]]})
      # {:multi, _} carrying something that is not an Ecto.Multi.
      refute EctoTarget.valid_result?({:multi, %{}})
      # Bare values a handler might return by accident.
      refute EctoTarget.valid_result?(:ok)
      refute EctoTarget.valid_result?(nil)
      refute EctoTarget.valid_result?(%ReadModel{})
    end
  end

  describe "init/1" do
    test "fetches :repo and stores it in state" do
      assert {:ok, %{repo: SomeRepo}} = EctoTarget.init(repo: SomeRepo)
    end

    test "raises when :repo is missing" do
      assert_raise KeyError, fn -> EctoTarget.init([]) end
    end
  end
end
