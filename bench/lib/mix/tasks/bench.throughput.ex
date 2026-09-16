defmodule Mix.Tasks.Bench.Throughput do
  @shortdoc "Measures projection catch-up throughput against a real event store"

  @moduledoc """
  Measures how fast a Scriba projection catches up on history it has never
  seen — the number that decides whether a rebuild or a post-outage
  catch-up is minutes or days.

      mix bench.throughput                        # 2000 events over 50 streams
      mix bench.throughput --events 5000 --streams 100
      mix bench.throughput --reseed               # append a fresh batch first

  Method: append N events to a real EventStore, truncate the read model and
  Scriba's cursor tables, then start the projection from `:origin` under a
  subscription name never used before, and time how long every event takes
  to reach the read model. Each run subscribes fresh, so runs are
  independent and repeatable.

  Source options can be passed through to the subscription, which is what
  makes before/after comparisons possible:

      mix bench.throughput --source-opt buffer_size=100

  A `--source-opt` the engine does not forward reaches nothing; that is
  itself a result worth recording.
  """

  use Mix.Task

  alias ScribaBench.{EventStore, ReadModel, Repo}
  alias ScribaBench.Events.Ticked

  @impl true
  def run(argv) do
    {opts, _, _} =
      OptionParser.parse(argv,
        strict: [
          events: :integer,
          streams: :integer,
          reseed: :boolean,
          timeout: :integer,
          source_opt: :keep
        ]
      )

    total = Keyword.get(opts, :events, 2_000)
    streams = Keyword.get(opts, :streams, 50)
    timeout_ms = Keyword.get(opts, :timeout, 600) * 1_000

    Mix.Task.run("app.start")

    source_opts = parse_source_opts(opts)

    stored = count_stored_events()

    if Keyword.get(opts, :reseed, false) or stored < total do
      seed(total, streams)
    else
      IO.puts("Event store already holds #{stored} events; reusing (--reseed to append more).")
    end

    reset_read_side()

    subscription = "bench-#{System.unique_integer([:positive])}"

    source =
      {Scriba.Source.Commanded,
       [application: ScribaBench.CommandedApp, subscription_name: subscription, start_from: :origin] ++
         source_opts}

    IO.puts("""

    Catching up #{total} events over #{streams} streams
      subscription: #{subscription}
      source opts:  #{inspect(source_opts)}
    """)

    started = System.monotonic_time(:millisecond)
    {:ok, _pid} = Scriba.start_projection(ScribaBench.Projection, source: source)

    case await_rows(total, started, timeout_ms) do
      {:ok, elapsed_ms} ->
        report(total, elapsed_ms)

      {:timeout, rows, elapsed_ms} ->
        IO.puts("""
        TIMED OUT after #{div(elapsed_ms, 1000)}s with #{rows}/#{total} events projected.
        Partial rate: #{rate(rows, elapsed_ms)} events/sec
        """)
    end

    Scriba.stop(ScribaBench.Projection)
  end

  defp parse_source_opts(opts) do
    opts
    |> Keyword.get_values(:source_opt)
    |> Enum.map(fn pair ->
      [k, v] = String.split(pair, "=", parts: 2)

      value =
        case Integer.parse(v) do
          {int, ""} -> int
          _ -> String.to_atom(v)
        end

      {String.to_atom(k), value}
    end)
  end

  defp count_stored_events do
    EventStore.stream_all_forward()
    |> Enum.count()
  rescue
    _ -> 0
  end

  # Appends in chunks per stream. Seeding cost is not part of the
  # measurement — the timer starts after this returns.
  defp seed(total, streams) do
    IO.puts("Seeding #{total} events across #{streams} streams...")
    started = System.monotonic_time(:millisecond)

    per_stream = div(total, streams)

    Enum.each(1..streams, fn s ->
      stream_uuid = "bench-stream-#{s}-#{System.unique_integer([:positive])}"

      events =
        Enum.map(1..per_stream, fn n ->
          %Commanded.EventStore.EventData{
            causation_id: Commanded.UUID.uuid4(),
            correlation_id: Commanded.UUID.uuid4(),
            event_type: "Elixir.ScribaBench.Events.Ticked",
            data: %Ticked{stream: stream_uuid, n: n},
            metadata: %{}
          }
        end)

      :ok =
        Commanded.EventStore.append_to_stream(
          ScribaBench.CommandedApp,
          stream_uuid,
          :any_version,
          events
        )
    end)

    elapsed = System.monotonic_time(:millisecond) - started
    IO.puts("Seeded in #{elapsed}ms (#{rate(per_stream * streams, elapsed)} events/sec append).")
  end

  defp reset_read_side do
    Ecto.Adapters.SQL.query!(
      Repo,
      "TRUNCATE bench_rows, scriba_positions, scriba_dead_letters",
      []
    )
  end

  defp await_rows(total, started, timeout_ms) do
    rows = Repo.aggregate(ReadModel, :count)
    elapsed = System.monotonic_time(:millisecond) - started

    cond do
      rows >= total -> {:ok, elapsed}
      elapsed > timeout_ms -> {:timeout, rows, elapsed}
      true -> poll_again(total, started, timeout_ms, rows, elapsed)
    end
  end

  defp poll_again(total, started, timeout_ms, rows, elapsed) do
    # Progress line every ~5s so a slow run is visibly alive rather than
    # silently hung.
    if rem(div(elapsed, 1000), 5) == 0 and rows > 0 do
      IO.write("\r  #{rows}/#{total} projected (#{rate(rows, elapsed)} ev/s)   ")
    end

    Process.sleep(200)
    await_rows(total, started, timeout_ms)
  end

  defp report(total, elapsed_ms) do
    IO.puts("""

    #{total} events projected in #{elapsed_ms}ms
    Throughput: #{rate(total, elapsed_ms)} events/sec

    Reference points at this rate:
      1M events:  #{humanize(1_000_000, total, elapsed_ms)}
      10M events: #{humanize(10_000_000, total, elapsed_ms)}
    """)
  end

  defp rate(_count, 0), do: "n/a"
  defp rate(count, elapsed_ms), do: Float.round(count * 1000 / elapsed_ms, 1)

  defp humanize(target, count, elapsed_ms) do
    seconds = target * (elapsed_ms / 1000) / max(count, 1)

    cond do
      seconds < 120 -> "#{Float.round(seconds, 1)}s"
      seconds < 7200 -> "#{Float.round(seconds / 60, 1)} min"
      seconds < 172_800 -> "#{Float.round(seconds / 3600, 1)} hours"
      true -> "#{Float.round(seconds / 86_400, 1)} days"
    end
  end
end
