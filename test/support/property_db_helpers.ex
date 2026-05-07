defmodule Scriba.Test.PropertyDbHelpers do
  @moduledoc """
  Shared helpers for `test/property_db/` — the real-Postgres property
  tests landing in –3.x.

  Compiled into the `:test` env only (under `test/support/`).
  """

  alias Ecto.Adapters.SQL.Sandbox
  alias Scriba.Test.Repo

  @doc """
  Sandbox setup for non-async tests that exercise the full projection
  process tree.

  Checks out a connection for the test process, then puts the sandbox
  into `{:shared, self()}` mode — every other process (Coordinator,
  Pipeline, Broadway producer/processors/batchers, the source, the Ecto
  target's transactions) sees the test's connection and shares its
  transaction. ExUnit's sandbox auto-checks-in on test exit; no
  `stop_owner` plumbing needed.

  ## Usage

      defmodule MyDbTest do
        use ExUnit.Case, async: false
        @moduletag :property_db

        setup :setup_sandbox

        defp setup_sandbox(ctx),
          do: Scriba.Test.PropertyDbHelpers.setup_sandbox(ctx)

        test "..." do
          # All processes spawned during this test share the connection.
        end
      end

  ## Why not async

  Shared-mode sandbox is incompatible with `async: true`: every async
  test would see every other test's data. PD1/PD2/PD3 are all `async:
  false`. If you need parallel property_db tests, switch to per-process
  `Sandbox.allow/3` with explicit ownership; that's deferred until it's
  load-bearing.
  """
  @spec setup_sandbox(map()) :: :ok
  def setup_sandbox(_context) do
    :ok = Sandbox.checkout(Repo)
    Sandbox.mode(Repo, {:shared, self()})
    :ok
  end

  @doc """
  Returns events with their `:id` prefixed by `name`, so each property-test
  iteration's read-model writes can be scoped via
  `event_id LIKE '<name>-%'`. The shared sandbox keeps every iteration in
  one big rolled-back transaction; this prefix is what disambiguates
  iterations within that transaction.
  """
  @spec scope_event_ids([Scriba.Event.t()], String.t()) :: [Scriba.Event.t()]
  def scope_event_ids(events, name) do
    Enum.map(events, fn event -> %{event | id: "#{name}-#{event.id}"} end)
  end

  @doc """
  Telemetry forwarder for `[:broadway, :batch_processor, :stop]`. Sends
  `{ref, :batch_done, prefix}` to `test_pid`.

  Including `prefix` in the message is structural protection against
  cross-iteration mailbox contamination: even if a previous iteration's
  late telemetry messages linger after its `wait_for_read_model/4`
  returns, the next iteration's receive matches on its own prefix and
  ignores them.

  Attach with config `%{test_pid: self(), ref: make_ref(), prefix: name}`.
  """
  def forward_batch_done(_event, _measurements, _metadata, %{
        test_pid: pid,
        ref: ref,
        prefix: prefix
      }) do
    send(pid, {ref, :batch_done, prefix})
  end

  @doc """
  Wait until `test_read_models` contains `expected_count` rows scoped to
  `name_prefix` (rows whose `event_id` starts with `<name_prefix>-`).

  Loops on `[:broadway, :batch_processor, :stop]` telemetry signals: each
  matching one triggers a DB count check. Returns `:ok` when the count is
  met, raises on timeout.

  No `Process.sleep/1` — every wakeup is a real Broadway commit signal
  matched on both `ref` and `prefix`.

  `name_prefix` is interpolated into a SQL `LIKE` pattern; `%` and `_`
  in it would silently widen the match. Asserts they're absent rather
  than escaping, since this helper's prefixes are caller-controlled and
  the constraint is cheap to enforce.
  """
  @spec wait_for_read_model(reference(), String.t(), non_neg_integer(), pos_integer()) :: :ok
  def wait_for_read_model(ref, name_prefix, expected_count, timeout_ms \\ 10_000) do
    if String.contains?(name_prefix, ["%", "_"]) do
      raise ArgumentError,
            "name_prefix must not contain LIKE wildcards % or _: #{inspect(name_prefix)}"
    end

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_read_model(ref, name_prefix, expected_count, deadline)
  end

  defp do_wait_for_read_model(ref, name_prefix, expected, deadline) do
    if read_model_count(name_prefix) >= expected do
      :ok
    else
      remaining = max(0, deadline - System.monotonic_time(:millisecond))

      receive do
        {^ref, :batch_done, ^name_prefix} ->
          do_wait_for_read_model(ref, name_prefix, expected, deadline)
      after
        remaining ->
          got = read_model_count(name_prefix)

          raise """
          Timed out waiting for read-model rows.
            expected: #{expected}
            got:      #{got}
            prefix:   #{name_prefix}
          """
      end
    end
  end

  @doc """
  Returns the count of `test_read_models` rows whose `event_id` starts
  with `<name_prefix>-`. Same query the wait function uses internally —
  exposed publicly for test assertions on accumulated rows.

  As with `wait_for_read_model/4`, `name_prefix` must not contain SQL
  `LIKE` wildcards.
  """
  @spec read_model_count(String.t()) :: non_neg_integer()
  def read_model_count(name_prefix) do
    if String.contains?(name_prefix, ["%", "_"]) do
      raise ArgumentError,
            "name_prefix must not contain LIKE wildcards % or _: #{inspect(name_prefix)}"
    end

    %{rows: [[count]]} =
      Ecto.Adapters.SQL.query!(
        Repo,
        "SELECT count(*) FROM test_read_models WHERE event_id LIKE $1",
        [name_prefix <> "-%"]
      )

    count
  end

  @doc """
  Looks up the running `Scriba.Test.Source` (Broadway producer) pid for
  a projection.

  Broadway names internal processes via `process_name(broadway_name,
  base_name)`; for `concurrency: 1` producers it's called with
  `base_name = "Producer_0"` (Broadway 1.3 — see
  `Broadway.Topology.process_name/3` and `process_names/3`,
  `deps/broadway/lib/broadway/topology.ex` lines 475–504, which
  interpolate `"\#{type}_\#{index}"`).

  Our `Scriba.Projection.Pipeline.process_name/2` returns
  `{:via, Registry, {Scriba.Internals.Registry, {name, version, suffix}}}`,
  so the producer is registered under key `{name, version, "Producer_0"}`.

  This lookup is **load-bearing on the Broadway internal naming
  convention**. If a future Broadway version changes the producer suffix
  format, this helper raises and tests fail loudly — preferable to silent
  drift.
  """
  @spec lookup_source(String.t(), pos_integer()) :: pid()
  def lookup_source(name, version) do
    case Registry.lookup(Scriba.Internals.Registry, {name, version, "Producer_0"}) do
      [{pid, _}] ->
        pid

      [] ->
        raise """
        Scriba.Test.Source producer not found for #{inspect({name, version})}.

        If this is a fresh Broadway version, verify the producer's
        process_name suffix is still "Producer_0" (see
        Broadway.Topology in deps/broadway). Otherwise, the projection
        may not be running yet — call this AFTER start_supervised!
        returns.
        """
    end
  end
end
