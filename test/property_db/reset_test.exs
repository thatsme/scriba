defmodule Scriba.PropertyDb.ResetTest do
  @moduledoc """
  `Scriba.reset/2` — the last step of a rebuild.

  Two properties carry the weight. It has to clear everything that makes a
  version remember where it was, including the ETS cache: a reset that
  cleared the table but left the cache would leave the next start deduping
  against positions that no longer exist, and silently skip the history it
  was supposed to replay. And it has to refuse while the projection runs,
  because clearing cursors under a live pipeline lets it commit against a
  cache that no longer matches the table.
  """

  use ExUnit.Case, async: false

  @moduletag :property_db

  alias Scriba.Test.PropertyDbHelpers
  alias Scriba.Test.Repo

  setup :setup_sandbox

  defp setup_sandbox(ctx), do: PropertyDbHelpers.setup_sandbox(ctx)

  setup do
    %{name: "reset-#{System.unique_integer([:positive])}"}
  end

  defp seed(name, version \\ 1) do
    projection = %{name: name, version: version}

    Ecto.Adapters.SQL.query!(
      Repo,
      """
      INSERT INTO scriba_positions
        (projection_name, projection_version, stream_id, position, updated_at)
      VALUES ($1, $2, 's-1', 10, now()), ($1, $2, 's-2', 20, now())
      """,
      [name, version]
    )

    :ok = Scriba.Watermark.put(Repo, projection, 20, DateTime.utc_now())

    event = %Scriba.Event{
      id: "e-#{System.unique_integer([:positive])}",
      stream_id: "s-1",
      type: "Elixir.Some.Event",
      data: %{},
      metadata: %{},
      position: 10,
      occurred_at: DateTime.utc_now()
    }

    {:ok, _} = Scriba.DeadLetter.insert(Repo, projection, event, {:error, :nope})

    Scriba.Position.cache_put(name, version, "s-1", 10)

    projection
  end

  defp positions(name, version) do
    %{rows: [[n]]} =
      Ecto.Adapters.SQL.query!(
        Repo,
        "SELECT count(*) FROM scriba_positions WHERE projection_name = $1 AND projection_version = $2",
        [name, version]
      )

    n
  end

  test "clears cursors, watermark and cache", %{name: name} do
    projection = seed(name)

    assert positions(name, 1) == 2
    assert %{position: 20} = Scriba.Watermark.get(Repo, projection)

    assert {:ok, counts} = Scriba.reset(name, repo: Repo)

    assert counts.positions == 2
    assert counts.watermark == 1
    assert positions(name, 1) == 0
    assert Scriba.Watermark.get(Repo, projection) == nil
    assert Scriba.Position.cache_get(name, 1, "s-1") == :error
  end

  test "keeps dead letters by default — they are why you are rebuilding", %{name: name} do
    projection = seed(name)

    assert {:ok, %{dead_letters: 0}} = Scriba.reset(name, repo: Repo)
    assert Scriba.DeadLetter.count(Repo, projection) == 1
  end

  test "deletes dead letters on request", %{name: name} do
    projection = seed(name)

    assert {:ok, %{dead_letters: 1}} = Scriba.reset(name, repo: Repo, dead_letters: true)
    assert Scriba.DeadLetter.count(Repo, projection) == 0
  end

  test "touches only the version it was asked about", %{name: name} do
    seed(name, 1)
    seed(name, 2)

    assert {:ok, _} = Scriba.reset(name, version: 1, repo: Repo)

    assert positions(name, 1) == 0
    assert positions(name, 2) == 2
  end

  test "resetting a projection that never ran is not an error", %{name: name} do
    assert {:ok, %{positions: 0, watermark: 0}} = Scriba.reset(name, repo: Repo)
  end

  test "the name form needs a repo" do
    assert Scriba.reset("whatever") == {:error, :no_repo}
  end

  describe "while the projection is running" do
    defmodule Projection do
      @moduledoc false
      use Scriba.Projection,
        name: "reset-guard",
        source: {Scriba.Test.Source, []},
        target: {Scriba.Target.Ecto, repo: Scriba.Test.Repo},
        parallelism: 1

      def handle(_event, _meta), do: :skip
    end

    setup do
      # stop/1 deregisters asynchronously, so a test that starts this
      # projection can collide with the previous test's teardown. Calling it
      # on one that is already gone exits, so check first.
      ensure_stopped()
      :ok
    end

    test "refuses, and says which state it is in" do
      {:ok, _pid} = Scriba.start_projection(Projection)
      on_exit(&ensure_stopped/0)

      assert {:error, {:running, state}} = Scriba.reset(Projection)
      assert state in [:initializing, :running]
    end

    test "allows it once stopped" do
      {:ok, _pid} = Scriba.start_projection(Projection)

      # stop/1 is rejected from :initializing, so wait until the pipeline is
      # actually up before asking it to stop.
      assert wait_until(fn -> match?({:ok, %{status: :running}}, Scriba.info(Projection)) end)
      :ok = Scriba.stop(Projection)

      # stop/1 does not remove the projection: the Coordinator stays
      # registered in a terminal :stopped state. Reset has to accept that as
      # stopped, or it would refuse the normal case.
      assert wait_until(fn ->
               match?({:ok, %{status: :stopped}}, Scriba.info(Projection))
             end)

      assert {:ok, _counts} = Scriba.reset(Projection)
    end
  end

  defp ensure_stopped do
    case Scriba.info(Scriba.PropertyDb.ResetTest.Projection) do
      {:ok, _} -> Scriba.stop(Scriba.PropertyDb.ResetTest.Projection)
      _ -> :ok
    end

    # stop/1 leaves the projection as a child of Scriba.Projections.Supervisor
    # — the Coordinator sits in :stopped rather than exiting — so starting it
    # again fails with :already_started until the child is removed.
    for {_, pid, _, _} <- DynamicSupervisor.which_children(Scriba.Projections.Supervisor) do
      DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
    end

    wait_until(fn ->
      DynamicSupervisor.which_children(Scriba.Projections.Supervisor) == []
    end)
  end

  defp wait_until(fun, attempts \\ 50)

  defp wait_until(fun, attempts) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> (fn -> Process.sleep(50) end).() && wait_until(fun, attempts - 1)
    end
  end
end
