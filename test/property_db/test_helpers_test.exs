defmodule Scriba.PropertyDb.TestHelpersTest do
  @moduledoc """
  `Scriba.Testing` against a real database.

  The helpers exist so a user can assert on read-model rows without starting
  a pipeline, so the assertions here are the ones a user would write: call
  the projection, then query the repo. Running them against real Postgres
  rather than a double is the point — the value of the helper is that it
  commits through the same target the engine uses, and only a real repo shows
  whether it does.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  import Ecto.Query

  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.ReadModel
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  defmodule Event do
    @moduledoc false
    defstruct [:id, :value]
  end

  defmodule Ignored do
    @moduledoc false
    defstruct [:id]
  end

  defmodule Projection do
    @moduledoc false
    use Scriba.Projection,
      name: "helpers-under-test",
      source: {Scriba.Test.Source, []},
      target: {Scriba.Target.Ecto, repo: Scriba.Test.Repo},
      parallelism: 1

    alias Scriba.PropertyDb.TestHelpersTest.Event

    def handle(%Event{value: :boom}, _meta), do: raise(ArgumentError, "handler exploded")
    def handle(%Event{value: :refuse}, _meta), do: {:error, :refused}
    def handle(%Event{value: :nonsense}, _meta), do: {:not, :a, :valid, :shape}

    def handle(%Event{id: id}, meta) do
      {:insert, %ReadModel{event_id: id, stream_id: meta.stream_id, position: meta.position}}
    end

    def handle(_other, _meta), do: :skip
  end

  describe "handle/3" do
    test "returns the handler's result without touching the database" do
      assert {:insert, %ReadModel{event_id: "e1", stream_id: "scriba-test", position: 1}} =
               Scriba.Testing.handle(Projection, %Event{id: "e1"})

      assert count() == 0
    end

    test "meta is overridable, and the handler reads it" do
      assert {:insert, %ReadModel{stream_id: "acc-9", position: 42}} =
               Scriba.Testing.handle(Projection, %Event{id: "e1"},
                 stream_id: "acc-9",
                 position: 42
               )
    end

    test "an ignored event type returns :skip" do
      assert :skip = Scriba.Testing.handle(Projection, %Ignored{id: "x"})
    end

    test "a raising handler is returned, not re-raised" do
      assert {:exception, %ArgumentError{message: "handler exploded"}, _stack} =
               Scriba.Testing.handle(Projection, %Event{id: "e1", value: :boom})
    end

    test "a module that is not a projection says so" do
      assert_raise ArgumentError, ~r/not a Scriba projection/, fn ->
        Scriba.Testing.handle(URI, %Event{id: "e1"})
      end
    end
  end

  describe "project/3" do
    test "commits read-model rows the caller can query" do
      result =
        Scriba.Testing.project(Projection, [
          %Event{id: "a"},
          %Event{id: "b"}
        ])

      assert %Scriba.Testing.Result{committed: 2, failed: [], invalid: [], skipped: []} = result
      assert count() == 2
      assert Repo.get(ReadModel, "a").position == 1
      assert Repo.get(ReadModel, "b").position == 2
    end

    test "advances the cursor per stream, skipping none" do
      Scriba.Testing.project(Projection, [
        {%Event{id: "a"}, stream_id: "s-1"},
        {%Event{id: "b"}, stream_id: "s-2"},
        {%Event{id: "c"}, stream_id: "s-1"}
      ])

      assert cursor("s-1") == 3
      assert cursor("s-2") == 2
    end

    test "a :skip commits nothing and leaves its stream's cursor alone" do
      result =
        Scriba.Testing.project(
          Projection,
          [%Ignored{id: "x"}],
          stream_id: "quiet"
        )

      assert %Scriba.Testing.Result{committed: 0, skipped: [{%Ignored{}, _meta}]} = result
      assert count() == 0
      assert cursor("quiet") == nil
    end

    test "handler failures are reported rather than routed" do
      result =
        Scriba.Testing.project(Projection, [
          %Event{id: "ok"},
          %Event{id: "bad", value: :refuse},
          %Event{id: "worse", value: :boom}
        ])

      assert result.committed == 1

      assert [{%Event{id: "bad"}, :refused}, {%Event{id: "worse"}, %ArgumentError{}}] =
               result.failed

      # The good event still committed: this is one transaction, and a
      # reported failure is not a rollback.
      assert count() == 1
    end

    test "a return the target cannot apply is reported as invalid" do
      result = Scriba.Testing.project(Projection, [%Event{id: "junk", value: :nonsense}])

      assert %Scriba.Testing.Result{
               committed: 0,
               invalid: [{%Event{id: "junk"}, {:not, _, _, _}}]
             } =
               result

      assert count() == 0
    end

    test "start_position controls the first event's position" do
      Scriba.Testing.project(Projection, [%Event{id: "a"}], start_position: 100)

      assert Repo.get(ReadModel, "a").position == 100
    end
  end

  defp count do
    Repo.aggregate(from(r in ReadModel, where: like(r.event_id, "%")), :count)
  end

  defp cursor(stream_id) do
    Repo.one(
      from(p in "scriba_positions",
        where: p.projection_name == "helpers-under-test" and p.stream_id == ^stream_id,
        select: p.position
      )
    )
  end
end
