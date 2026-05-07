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

  describe "build_multi/4 — handler return shapes (§4.2)" do
    test ":skip yields no event op, only the per-stream position update" do
      events = [event("e1", 1, "stream-a")]
      multi = EctoTarget.build_multi(events, [:skip], projection(), advances_for(events))

      assert keys(multi) == [{:scriba_position, "stream-a"}]
    end

    test "{:insert, struct} adds an insert step keyed by event id" do
      events = [event("e1", 1, "stream-a")]
      result = {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events))

      assert {:scriba_event, "e1"} in keys(multi)
      assert {:scriba_position, "stream-a"} in keys(multi)
    end

    test "{:update, schema, filter, [set: changes]} adds an update_all step" do
      events = [event("e1", 1, "stream-a")]
      result = {:update, ReadModel, [event_id: "e1"], set: [position: 99]}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events))

      assert {:scriba_event, "e1"} in keys(multi)
    end

    test "{:delete, schema, filter} adds a delete_all step" do
      events = [event("e1", 1, "stream-a")]
      result = {:delete, ReadModel, [event_id: "e1"]}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events))

      assert {:scriba_event, "e1"} in keys(multi)
    end

    test "{:multi, user_multi} merges the user's Multi" do
      events = [event("e1", 1, "stream-a")]
      user_multi = Ecto.Multi.new() |> Ecto.Multi.run(:user_op, fn _, _ -> {:ok, :done} end)
      result = {:multi, user_multi}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events))

      ks = keys(multi)
      assert {:scriba_position, "stream-a"} in ks
      assert :merge in ks
    end

    test "{:error, reason} adds a step that fails the transaction" do
      events = [event("e1", 1, "stream-a")]
      result = {:error, :boom}
      multi = EctoTarget.build_multi(events, [result], projection(), advances_for(events))

      assert {:scriba_event, "e1"} in keys(multi)
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

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events))
      ks = keys(multi)

      assert {:scriba_event, "e1"} in ks
      refute {:scriba_event, "e2"} in ks
      assert {:scriba_event, "e3"} in ks
      assert {:scriba_position, "stream-a"} in ks
    end
  end

  describe "build_multi/4 — multi-stream batches" do
    test "appends one position step per distinct stream in the batch" do
      events = [
        event("e1", 1, "stream-a"),
        event("e2", 2, "stream-b"),
        event("e3", 3, "stream-a")
      ]

      results = Enum.map(events, fn _ -> :skip end)
      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events))
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

      multi = EctoTarget.build_multi(events, results, projection(), advances)
      assert {:scriba_position, "stream-a"} in keys(multi)
    end
  end

  describe "build_multi/4 — position updates appended after handler ops" do
    test "all per-stream position steps come after handler steps" do
      events = [event("e1", 1, "stream-a"), event("e2", 2, "stream-b")]
      results = [
        {:insert, %ReadModel{event_id: "e1", stream_id: "stream-a", position: 1}},
        {:insert, %ReadModel{event_id: "e2", stream_id: "stream-b", position: 2}}
      ]

      multi = EctoTarget.build_multi(events, results, projection(), advances_for(events))
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
