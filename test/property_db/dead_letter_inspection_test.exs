defmodule Scriba.PropertyDb.DeadLetterInspectionTest do
  @moduledoc """
  Reading dead letters back.

  Writing them was already covered; this is the other half, and it is the
  half an operator uses at three in the morning. Against real Postgres
  because the filters, ordering and grouping are SQL — a double would prove
  the function signatures and nothing about the queries.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.DeadLetter
  alias Scriba.Event
  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  setup do
    %{projection: %{name: "dl-#{System.unique_integer([:positive])}", version: 1}}
  end

  defp write(projection, opts) do
    event = %Event{
      id: Keyword.get(opts, :id, "e-#{System.unique_integer([:positive])}"),
      stream_id: Keyword.get(opts, :stream_id, "s-1"),
      type: Keyword.get(opts, :type, "Elixir.Some.Event"),
      data: %{payload: "x"},
      metadata: %{},
      position: Keyword.get(opts, :position, 1),
      occurred_at: DateTime.utc_now()
    }

    {:ok, _} =
      DeadLetter.insert(Repo, projection, event, Keyword.get(opts, :error, {:error, :nope}))
  end

  test "lists nothing for a projection with no dead letters", %{projection: p} do
    assert DeadLetter.list(Repo, p) == []
    assert DeadLetter.count(Repo, p) == 0
    assert %{total: 0, by_error_kind: %{}, oldest: nil, newest: nil} = DeadLetter.stats(Repo, p)
  end

  test "lists rows with the fields an operator needs", %{projection: p} do
    write(p, id: "e-1", stream_id: "order-7", position: 99, type: "Elixir.OrderPlaced")

    assert [row] = DeadLetter.list(Repo, p)
    assert row.position == 99
    assert row.stream_id == "order-7"
    assert row.event_type == "Elixir.OrderPlaced"
    assert row.error_kind == "error"
    assert row.error_message =~ "nope"
    assert %{"payload" => "x"} = row.event_data
    assert row.occurred_at
  end

  test "newest first by default, oldest first on request", %{projection: p} do
    write(p, id: "old", position: 1)
    Process.sleep(10)
    write(p, id: "new", position: 2)

    assert [%{position: 2}, %{position: 1}] = DeadLetter.list(Repo, p)
    assert [%{position: 1}, %{position: 2}] = DeadLetter.list(Repo, p, order: :asc)
  end

  test "filters by stream, kind and time", %{projection: p} do
    write(p, stream_id: "a", position: 1)
    write(p, stream_id: "b", position: 2)
    write(p, stream_id: "b", position: 3, error: {:exception, %ArgumentError{}, []})

    assert [%{stream_id: "a"}] = DeadLetter.list(Repo, p, stream_id: "a")
    assert DeadLetter.count(Repo, p, stream_id: "b") == 2
    assert DeadLetter.count(Repo, p, error_kind: "Elixir.ArgumentError") == 1

    assert DeadLetter.count(Repo, p, since: DateTime.add(DateTime.utc_now(), 60, :second)) == 0
    assert DeadLetter.count(Repo, p, since: DateTime.add(DateTime.utc_now(), -60, :second)) == 3
  end

  test "pages", %{projection: p} do
    for n <- 1..5, do: write(p, position: n)

    assert [_, _] = DeadLetter.list(Repo, p, limit: 2)
    assert [_] = DeadLetter.list(Repo, p, limit: 2, offset: 4)
    assert DeadLetter.count(Repo, p) == 5
  end

  test "stats distinguish a poison event from a systemic failure", %{projection: p} do
    # One kind spread across streams: the handler or schema is wrong.
    for n <- 1..4 do
      write(p, stream_id: "s-#{n}", position: n, error: {:error, :constraint})
    end

    # One kind on one stream: a single bad event.
    write(p, stream_id: "s-1", position: 9, error: {:exception, %ArgumentError{}, []})

    stats = DeadLetter.stats(Repo, p)

    assert stats.total == 5
    assert stats.by_error_kind["error"] == 4
    assert stats.by_error_kind["Elixir.ArgumentError"] == 1
    assert DateTime.compare(stats.newest, stats.oldest) in [:gt, :eq]
  end

  test "one projection cannot see another's", %{projection: p} do
    other = %{name: "#{p.name}-other", version: 1}
    write(p, position: 1)

    assert DeadLetter.count(Repo, p) == 1
    assert DeadLetter.count(Repo, other) == 0
  end

  test "versions of the same projection are separate", %{projection: p} do
    v2 = %{p | version: 2}
    write(p, position: 1)

    assert DeadLetter.count(Repo, p) == 1
    assert DeadLetter.count(Repo, v2) == 0
  end

  describe "the Scriba.* entry points" do
    defmodule Projection do
      @moduledoc false
      use Scriba.Projection,
        name: "dl-entrypoint",
        source: {Scriba.Test.Source, []},
        target: {Scriba.Target.Ecto, repo: Scriba.Test.Repo},
        parallelism: 1

      def handle(_event, _meta), do: :skip
    end

    test "read through the projection module, which knows its own repo" do
      projection = %{name: "dl-entrypoint", version: 1}
      write(projection, position: 5)

      assert [%{position: 5}] = Scriba.dead_letters(Projection)
      assert %{total: 1} = Scriba.dead_letter_stats(Projection)
    end

    test "the name form needs a repo, and says so" do
      assert_raise ArgumentError, ~r/no repo/, fn ->
        Scriba.dead_letters("dl-entrypoint")
      end

      assert Scriba.dead_letters("dl-entrypoint", repo: Repo) == []
    end
  end
end
