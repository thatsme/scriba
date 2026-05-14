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

  describe "build_multi/5 — dead-letter routing" do
    test "{:error, _} dead-letter produces a :scriba_dead_letter step, NOT a failing transaction step" do
      # regression: the previous build_multi/4 inserted an
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

  describe "init/1" do
    test "fetches :repo and stores it in state" do
      assert {:ok, %{repo: SomeRepo}} = EctoTarget.init(repo: SomeRepo)
    end

    test "raises when :repo is missing" do
      assert_raise KeyError, fn -> EctoTarget.init([]) end
    end
  end
end
