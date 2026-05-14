defmodule Scriba.Source.CommandedTest do
  @moduledoc """
  Unit tests for `Scriba.Source.Commanded.to_message/2`'s
  RecordedEvent → Scriba.Event field mapping (regression coverage for
  the `stream_uuid` field-name bug), plus an integration test that
  exercises the subscription path against `Commanded.EventStore.Adapters.InMemory`.

  ## Note on InMemory adapter delivery cadence

  `Commanded.EventStore.Adapters.InMemory` delivers events to
  subscribers **one at a time** — each `{:events, _}` message contains
  exactly one event, and the next event is held until the previous one
  is acked via `Commanded.EventStore.ack_event/3`. This is different
  from the persistent adapter which typically batches deliveries.

  The integration test below uses an explicit `drain_subscription/4`
  helper that loops `receive ack receive ack ...` rather than expecting
  a single `{:events, [...]}` with all events. Scriba's own
  `Scriba.Source.Commanded` handles this correctly because its
  `dispatch/1` drains its `pending` queue against accumulated demand
  regardless of how messages arrive.
  """

  use ExUnit.Case, async: false

  # NOT aliasing Scriba.Source.Commanded as `Commanded` — that would
  # shadow the top-level `Commanded` namespace and break
  # `use Commanded.Application` in the integration describe.
  alias Scriba.Source.Commanded, as: ScribaCommanded

  describe "child_spec/1" do
    test "returns a worker spec without invoking Commanded" do
      spec = ScribaCommanded.child_spec(application: SomeApp, subscription_name: "test")

      assert spec.id == ScribaCommanded
      assert {ScribaCommanded, :start_link, [_opts]} = spec.start
      assert spec.type == :worker
      assert spec.restart == :permanent
    end
  end

  describe "to_message/2 — field mapping (regression for stream_uuid bug)" do
    # This describe pins the field mapping between
    # Commanded.EventStore.RecordedEvent and %Scriba.Event{}. The original
    # bug had `stream_id: commanded_event.stream_uuid` — but RecordedEvent
    # in Commanded 1.4 (and earlier) defines :stream_id, not :stream_uuid.
    # Pre-fix, this test would have failed: the RecordedEvent struct
    # construction below would compile (no stream_uuid field is set), and
    # accessing commanded_event.stream_uuid at runtime would raise KeyError.

    defp build_recorded_event(overrides \\ []) do
      defaults = [
        event_id: "event-uuid-1",
        event_number: 42,
        stream_id: "account-uuid-abc",
        stream_version: 1,
        causation_id: nil,
        correlation_id: nil,
        event_type: "Elixir.MyApp.Events.AccountOpened",
        data: %{some: "data"},
        created_at: ~U[2026-05-14 12:00:00.000000Z],
        metadata: %{actor_id: "user-42"}
      ]

      struct!(Commanded.EventStore.RecordedEvent, Keyword.merge(defaults, overrides))
    end

    defp build_state, do: %{application: :test_app, subscription: :test_sub}

    test "maps RecordedEvent.stream_id to Scriba.Event.stream_id (the regression)" do
      recorded = build_recorded_event(stream_id: "account-xyz")

      %Broadway.Message{data: %Scriba.Event{} = event} =
        ScribaCommanded.to_message(recorded, build_state())

      assert event.stream_id == "account-xyz"
      refute is_nil(event.stream_id),
             "stream_id is nil — to_message/2 likely reads the wrong field from RecordedEvent"
    end

    test "maps event_id, event_type, data, position, occurred_at correctly" do
      recorded =
        build_recorded_event(
          event_id: "evt-42",
          event_type: "OrderPlaced",
          data: %{order_id: "ord-1"},
          event_number: 99,
          created_at: ~U[2026-01-01 00:00:00.000000Z]
        )

      %Broadway.Message{data: %Scriba.Event{} = event} =
        ScribaCommanded.to_message(recorded, build_state())

      assert event.id == "evt-42"
      assert event.type == "OrderPlaced"
      assert event.data == %{order_id: "ord-1"}
      assert event.position == 99
      assert event.occurred_at == ~U[2026-01-01 00:00:00.000000Z]
    end

    test "preserves a non-empty metadata map" do
      recorded = build_recorded_event(metadata: %{correlation: "abc", user_id: 42})

      %Broadway.Message{data: %Scriba.Event{metadata: meta}} =
        ScribaCommanded.to_message(recorded, build_state())

      assert meta == %{correlation: "abc", user_id: 42}
    end

    test "coerces nil metadata to %{}" do
      # RecordedEvent.metadata defaults to %{} via defstruct, but some
      # event store implementations send nil. The contract is that
      # %Scriba.Event{}.metadata is always a map, never nil.
      recorded = build_recorded_event(metadata: nil)

      %Broadway.Message{data: %Scriba.Event{metadata: meta}} =
        ScribaCommanded.to_message(recorded, build_state())

      assert meta == %{}
    end

    test "acknowledger tuple carries application, subscription, and the original RecordedEvent" do
      recorded = build_recorded_event()

      %Broadway.Message{acknowledger: ack} =
        ScribaCommanded.to_message(recorded, %{application: :my_app, subscription: :sub_ref})

      assert {ScribaCommanded, %{application: :my_app, subscription: :sub_ref}, ^recorded} = ack
    end
  end

  describe "subscription path via InMemory adapter (integration)" do
    @describetag :integration

    defmodule App do
      use Commanded.Application,
        otp_app: :scriba,
        event_store: [adapter: Commanded.EventStore.Adapters.InMemory]
    end

    defmodule Event do
      defstruct [:n]
    end

    setup do
      # Each test gets a fresh InMemory event store via the app's
      # supervisor. start_supervised registers the cleanup.
      app_pid = start_supervised!(App)
      %{app_pid: app_pid}
    end

    test "events written via append_to_stream arrive as %Broadway.Message{} with correct shape" do
      # Subscribe via Commanded.EventStore.subscribe_to (the same facade
      # Scriba.Source.Commanded uses internally). This skips Broadway's
      # GenStage producer wrapping but verifies the underlying contract:
      # Commanded delivers RecordedEvent structs whose fields match what
      # to_message/2 reads.
      {:ok, subscription} =
        Commanded.EventStore.subscribe_to(App, :all, "test-sub", self(), :origin)

      assert_receive {:subscribed, ^subscription}, 1_000

      events =
        Enum.map(1..3, fn n ->
          %Commanded.EventStore.EventData{
            event_type: to_string(Event),
            data: struct!(Event, n: n),
            metadata: %{}
          }
        end)

      :ok =
        Commanded.EventStore.append_to_stream(
          App,
          "stream-account-xyz",
          0,
          events
        )

      # The InMemory adapter delivers events one at a time and waits for
      # each ack before sending the next. Drain by ack-and-receive until
      # we've seen all 3 or hit a timeout.
      drained = drain_subscription(App, subscription, 3, 1_000)

      assert length(drained) == 3
      [recorded | _] = drained

      # Verify the RecordedEvent shape matches what to_message/2 reads.
      # If Commanded ever renames a field, this fails loudly.
      assert is_binary(recorded.event_id)
      assert recorded.stream_id == "stream-account-xyz"
      assert recorded.event_number == 1
      assert recorded.event_type == "Elixir.Scriba.Source.CommandedTest.Event"
      assert %Event{n: 1} = recorded.data

      # End-to-end: feed the first event through to_message/2 and verify
      # the resulting Broadway.Message has the right Scriba.Event shape.
      %Broadway.Message{data: %Scriba.Event{} = scriba_event} =
        ScribaCommanded.to_message(recorded, %{application: App, subscription: subscription})

      assert scriba_event.id == recorded.event_id
      assert scriba_event.stream_id == "stream-account-xyz"
      assert scriba_event.type == "Elixir.Scriba.Source.CommandedTest.Event"
      assert scriba_event.position == 1
      assert %Event{n: 1} = scriba_event.data
    end

    # Drains up to `target_count` events from the subscription's mailbox,
    # acking each so the adapter releases the next. Returns events in
    # delivery order. Stops early if `timeout` ms elapse without a
    # delivery.
    defp drain_subscription(_app, _sub, 0, _timeout), do: []

    defp drain_subscription(app, sub, remaining, timeout) do
      receive do
        {:events, evts} ->
          last = List.last(evts)
          :ok = Commanded.EventStore.ack_event(app, sub, last)
          evts ++ drain_subscription(app, sub, remaining - length(evts), timeout)
      after
        timeout -> []
      end
    end
  end
end
