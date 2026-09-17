defmodule ScribaBench.WatermarkTest do
  @moduledoc """
  Does a running projection actually record where it has got to?

  The unit tests cover `Scriba.Watermark` against a database. What they
  cannot cover is the part that matters operationally: that the producer
  computes the same number it acknowledges, and persists it, against a real
  event store. The watermark is only trustworthy if it agrees with what was
  committed.

  Asserted here:

    * a caught-up projection's watermark equals the last event's position
    * `Scriba.info/2` reports it, with a lag derived from the event's own
      timestamp
    * the watermark never runs ahead of the read model — the property that
      makes it safe to resume from
  """

  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias ScribaBench.{CommandedApp, Repo}
  alias ScribaBench.Events.Ticked
  alias ScribaBench.Wait

  @events 300

  setup do
    reset_event_store()
    SQL.query!(Repo, "TRUNCATE bench_rows, scriba_positions, scriba_dead_letters", [])
    SQL.query!(Repo, "DELETE FROM scriba_watermarks", [])

    on_exit(fn ->
      for {_, pid, _, _} <- DynamicSupervisor.which_children(Scriba.Projections.Supervisor) do
        DynamicSupervisor.terminate_child(Scriba.Projections.Supervisor, pid)
      end
    end)

    :ok
  end

  test "a caught-up projection records the position it reached" do
    seed(@events)

    {:ok, _} =
      Scriba.start_projection(ScribaBench.Projection,
        source:
          {Scriba.Source.Commanded,
           application: CommandedApp,
           subscription_name: "wm-#{System.unique_integer([:positive])}",
           start_from: :origin,
           buffer_size: 500}
      )

    assert Wait.until(fn -> rows() == @events end, 60_000),
           "projection never caught up: #{rows()}/#{@events}"

    # The watermark is written on a throttle, so it lands shortly after the
    # last commit rather than with it.
    assert Wait.until(fn -> watermark_position() == @events end, 15_000),
           "watermark never reached #{@events}, stopped at #{inspect(watermark_position())}"

    {:ok, info} = Scriba.info(ScribaBench.Projection)

    assert info.watermark == @events
    assert is_integer(info.lag_ms) and info.lag_ms >= 0

    # Seeding happened moments ago, so the lag is small. The point is that it
    # is measured from the event's timestamp rather than from the write.
    assert info.lag_ms < 120_000
  end

  test "the watermark never runs ahead of what committed" do
    seed(@events)

    {:ok, _} =
      Scriba.start_projection(ScribaBench.Projection,
        source:
          {Scriba.Source.Commanded,
           application: CommandedApp,
           subscription_name: "wm-#{System.unique_integer([:positive])}",
           start_from: :origin,
           buffer_size: 50}
      )

    # Sample throughout the catch-up rather than only at the end: a watermark
    # that briefly overtakes the read model would be invisible afterwards, and
    # it is exactly what makes resuming from it lose events.
    violations =
      Enum.reduce(1..60, [], fn _, acc ->
        Process.sleep(100)

        case {watermark_position(), rows()} do
          {nil, _} -> acc
          {wm, committed} when wm > committed -> [{wm, committed} | acc]
          _ -> acc
        end
      end)

    assert violations == [],
           """
           The watermark ran ahead of the read model: #{inspect(Enum.take(violations, 5))}
           (as {watermark, committed rows}). A replica resuming from it would
           skip everything in between.
           """
  end

  ## Helpers

  defp seed(count) do
    Enum.each(1..count, fn n ->
      stream = "wm-#{rem(n, 25)}-#{System.unique_integer([:positive])}"

      event = %Commanded.EventStore.EventData{
        causation_id: Commanded.UUID.uuid4(),
        correlation_id: Commanded.UUID.uuid4(),
        event_type: "Elixir.ScribaBench.Events.Ticked",
        data: %Ticked{stream: stream, n: n},
        metadata: %{}
      }

      :ok = Commanded.EventStore.append_to_stream(CommandedApp, stream, :any_version, [event])
    end)
  end

  defp rows do
    %{rows: [[n]]} = SQL.query!(Repo, "SELECT count(*) FROM bench_rows", [])
    n
  end

  defp watermark_position do
    case SQL.query!(Repo, "SELECT position FROM scriba_watermarks", []) do
      %{rows: [[position]]} -> position
      _ -> nil
    end
  end

  defp reset_event_store do
    config = ScribaBench.EventStore.config()
    {:ok, conn} = Postgrex.start_link(config)
    EventStore.Storage.Initializer.reset!(conn, config)
    GenServer.stop(conn)
  end
end
