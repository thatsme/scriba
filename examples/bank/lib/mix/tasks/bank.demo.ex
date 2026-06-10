defmodule Mix.Tasks.Bank.Demo do
  @moduledoc """
  End-to-end demo of Scriba projecting a Commanded event stream.

  Sequence:

    1. Auto-reset: truncate read-model tables so the demo starts clean.
    2. Start the Bank application (Repo + Commanded + projection).
    3. Open 3 accounts.
    4. Dispatch 50 random deposit/withdraw commands across them.
    5. Poll `Scriba.info/1` until `safe_position == 53` — projection
       has caught up.
    6. Print the final read-model state.

  Failure modes:

    * Postgres not reachable → clear message, suggested fix, exits.
    * Migrations not run → suggests `mix bank.setup`, exits.
  """

  use Mix.Task

  @shortdoc "Run the bank demo end-to-end (open 3 accounts, 50 ops, print balances)"

  @account_count 3
  @op_count 50
  @total_events @account_count + @op_count

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start", [])

    auto_reset!()

    Mix.shell().info("=== Scriba Bank Demo ===\n")

    # Attach the event counter BEFORE dispatching so we don't miss any
    # :event :stop telemetry. The Bank.Projections.Starter has already
    # brought up the projection by this point (it's in
    # Bank.Application's children list), but no events have flowed yet
    # because we haven't dispatched anything.
    counter = start_event_counter()

    account_ids = open_accounts(@account_count)
    Mix.shell().info("Opened #{@account_count} accounts.")

    {ops_time_us, :ok} =
      :timer.tc(fn ->
        dispatch_random_ops(account_ids, @op_count)
        :ok
      end)

    Mix.shell().info(
      "Dispatched #{@op_count} deposit/withdraw commands in " <>
        format_duration(ops_time_us)
    )

    Mix.shell().info("Waiting for projection to catch up...")

    {wait_time_us, :ok} =
      :timer.tc(fn ->
        wait_for_event_count(counter, @total_events, 10_000)
      end)

    Mix.shell().info("Caught up in #{format_duration(wait_time_us)}.\n")

    print_balances()
    print_summary(counter)
  rescue
    e in DBConnection.ConnectionError ->
      die("""
      Cannot connect to Postgres: #{Exception.message(e)}

      Check that:
        * Postgres is running and reachable.
        * BANK_DEMO_DB_* env vars match your setup (see .env.local.example).

      Then run `mix bank.setup` to create the database and apply migrations.
      """)

    e in Postgrex.Error ->
      cond do
        e.postgres && e.postgres.code == :undefined_table ->
          die("""
          Required table is missing: #{e.postgres.message || ""}

          Run `mix bank.setup` to apply migrations, then re-run `mix bank.demo`.
          """)

        true ->
          reraise(e, __STACKTRACE__)
      end
  end

  ## Demo orchestration

  defp auto_reset! do
    Bank.Repo.query!("TRUNCATE account_balances, scriba_positions, scriba_dead_letters")
  end

  defp open_accounts(n) do
    Enum.map(1..n, fn _ ->
      account_id = Ecto.UUID.generate()
      :ok = Bank.CommandedApp.dispatch(%Bank.Commands.OpenAccount{account_id: account_id})
      account_id
    end)
  end

  defp dispatch_random_ops(account_ids, count) do
    Enum.each(1..count, fn _ ->
      account_id = Enum.random(account_ids)
      amount = Enum.random(10..50_000)

      cmd =
        case Enum.random([:deposit, :withdraw]) do
          :deposit ->
            %Bank.Commands.Deposit{account_id: account_id, amount_cents: amount}

          :withdraw ->
            %Bank.Commands.Withdraw{account_id: account_id, amount_cents: amount}
        end

      :ok = Bank.CommandedApp.dispatch(cmd)
    end)
  end

  # Why a counter instead of polling Scriba.info's safe_position:
  # safe_position is the MIN across stream cursors. With N events across
  # K streams, no stream's cursor ever reaches N (each stream's cursor
  # is its OWN last event's position, somewhere between K and N). The
  # min would settle below N. We'd time out.
  #
  # Counting [:scriba, :projection, :event, :stop] telemetry events is
  # exact: one event per handler invocation that succeeded. With no
  # dead-letters and no :skip returns in this demo, the count reaches
  # exactly @total_events when everything has committed.

  defp start_event_counter do
    counter = :counters.new(1, [])

    :telemetry.attach(
      {:bank_demo_counter, make_ref()},
      [:scriba, :projection, :event, :stop],
      &__MODULE__.handle_event_stop/4,
      counter
    )

    counter
  end

  @doc false
  def handle_event_stop(_event, _measurements, _metadata, counter) do
    :counters.add(counter, 1, 1)
  end

  defp wait_for_event_count(counter, target, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait(counter, target, deadline)
  end

  defp do_wait(counter, target, deadline) do
    seen = :counters.get(counter, 1)

    cond do
      seen >= target ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:ok, info} = Scriba.info(Bank.Projections.Balances)

        die("""
        Projection did not catch up within the timeout.
          events processed: #{seen} (expected: #{target})
          projection state: #{inspect(info.status)}
          safe_position:    #{info.safe_position}
        """)

      true ->
        Process.sleep(10)
        do_wait(counter, target, deadline)
    end
  end

  ## Output

  defp print_balances do
    rows =
      Bank.Repo.all(Bank.ReadModels.AccountBalance)
      |> Enum.sort_by(& &1.account_id)

    Mix.shell().info("account_balances:")

    Enum.each(rows, fn row ->
      Mix.shell().info(
        "  #{short_uuid(row.account_id)} balance: #{format_money(row.balance_cents)} USD" <>
          "  (last event: pos #{row.last_event_position})"
      )
    end)

    Mix.shell().info("")
  end

  # `counter` is the exact count of [:scriba, :projection, :event, :stop]
  # telemetry events the handler processed (reaches @total_events or the demo
  # would have died waiting). safe_position is the MIN across the per-account
  # stream cursors, so it sits below the processed total — that gap is
  # expected and is exactly what the counter-vs-safe_position comment above
  # explains. Print both: they answer different questions.
  defp print_summary(counter) do
    {:ok, info} = Scriba.info(Bank.Projections.Balances)

    Mix.shell().info("Total events processed: #{:counters.get(counter, 1)}")
    Mix.shell().info("Projection state:       #{info.status}")
    Mix.shell().info("Safe position:          #{info.safe_position}")
  end

  ## Formatting

  defp short_uuid(uuid), do: String.slice(uuid, 0, 8) <> "..."

  defp format_money(cents) when is_integer(cents) do
    sign = if cents < 0, do: "-", else: ""
    abs_cents = abs(cents)
    dollars = div(abs_cents, 100)
    rem_cents = rem(abs_cents, 100)
    "#{sign}#{dollars}.#{String.pad_leading(Integer.to_string(rem_cents), 2, "0")}"
  end

  defp format_duration(us) when us < 1_000, do: "#{us}µs"
  defp format_duration(us) when us < 1_000_000, do: "#{div(us, 1_000)}ms"
  defp format_duration(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  defp die(message) do
    Mix.shell().error("\n#{message}")
    exit({:shutdown, 1})
  end
end
