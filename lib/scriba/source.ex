defmodule Scriba.Source do
  @moduledoc """
  Behaviour for event sources.

  A source pulls events from somewhere — a Commanded event store, an external
  queue, an HTTP feed — and yields them as `Broadway.Message` values whose
  `:data` is a `Scriba.Event`. The engine plugs the source into a Broadway
  pipeline as the producer.

  Implementations must:

    * Implement this behaviour's `child_spec/1` and `start_link/1` so the
      source can be placed under a supervisor.
    * Implement `Broadway.Producer` so events can be consumed by the
      pipeline.
    * Implement `pause/1` and `resume/1` for the Coordinator's
      pause/resume lifecycle commands.

  ## Pause semantics

  `pause/1` means **stop yielding new events to the Pipeline.** What
  happens to the source's own buffering DURING pause is adapter-specific:

    * `Scriba.Test.Source` has a finite, bounded queue supplied at
      `start_link/1`. No growth during pause — the source simply stops
      consuming from its own queue.

    * `Scriba.Source.Commanded`'s EventStore subscription keeps pushing
      events into the source's `pending :queue`. Memory grows during
      pause, bounded by however many events the upstream produces in the
      pause window. This is a documented sharp edge. The
      cleaner alternative — unsubscribe on pause, re-subscribe on resume
      from the current cursor — is v0.5 hardening territory.

  A pause lives in the producer, not in the Coordinator, so a Pipeline that
  dies while the projection is paused would otherwise come back emitting: its
  replacement producer starts unpaused. The Coordinator reapplies the pause to
  the new producer before returning to `:paused`, so the pause survives a
  Pipeline restart. It does not survive a Coordinator restart, which is the
  process holding the instruction — that is a restart of the projection as a
  whole, and it comes back `:running`.

  The Coordinator's `pause/2` returns `:ok` once the pause signal is
  sent to the producer (asynchronous `send/2`). It does NOT wait for
  the source's `handle_info` to run. In-flight events already in
  Pipeline processors or batchers continue through their commit
  lifecycle per architecture §7. Operators should not assume "no
  commits possible" the instant `Scriba.pause/1` returns.
  """

  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()
  @callback start_link(opts :: keyword()) :: GenServer.on_start()

  @doc """
  Signals the source to stop yielding new events. Implementation MUST be
  non-blocking — Coordinator calls this synchronously from its state
  machine and cannot afford to wait on the source.

  Convention: `send(pid, :scriba_pause)` handled in the source's
  GenStage `handle_info/2`.
  """
  @callback pause(producer_pid :: pid()) :: :ok

  @doc """
  Signals the source to resume yielding new events. After resume, the
  source should drain any accumulated pending demand from its queue.

  Convention: `send(pid, :scriba_resume)` handled in the source's
  GenStage `handle_info/2`.
  """
  @callback resume(producer_pid :: pid()) :: :ok

  # Injected option, not a callback: the Pipeline adds `:scriba_watermark`
  # to every source's opts, carrying `repo:` and `projection:`. A source that
  # can compute a contiguous global position persists it there (see
  # `Scriba.Watermark`); one that cannot ignores the option.
end
