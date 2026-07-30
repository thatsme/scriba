defmodule Scriba.Projection.Pipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Scriba.Projection.Coordinator

  # Default retry policy per architecture §9.1:
  # 3 attempts, exponential backoff. The backoff list provides the sleeps
  # BETWEEN attempts (not before the first attempt, not after the last),
  # so N attempts require ≥ N-1 backoff entries. Default backoff has 3
  # entries — the third is unused at the default max_attempts=3 but
  # available if a user bumps max_attempts to 4 without overriding backoff.
  @default_retry %{max_attempts: 3, backoff: [100, 1000, 10_000]}

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)

    {source_module, source_opts} = Keyword.fetch!(opts, :source)
    {target_module, target_opts} = Keyword.fetch!(opts, :target)
    parallelism = Keyword.fetch!(opts, :parallelism)
    handler = Keyword.fetch!(opts, :handler)
    batch_size = Keyword.get(opts, :batch_size, 50)
    batch_timeout = Keyword.get(opts, :batch_timeout, 100)
    retry_config = parse_retry_opts(Keyword.get(opts, :retry))

    {:ok, target_state} = target_module.init(target_opts)

    # Same repo resolution as Coordinator — single source of truth in
    # Scriba.Position.resolve_repo/2. Used by source-side dedup's
    # cache_get/4 fallback when a stream's cursor isn't preloaded in cache.
    repo = Scriba.Position.resolve_repo(opts, {target_module, target_opts})

    # Capture parallelism in the partition_by closure so each call goes
    # through Scriba.Partitioner.partition/2 with the right partition count.
    # Broadway's `concurrency` and our partition function's modulus are kept
    # in lockstep this way.
    partition_by = fn %Message{data: %Scriba.Event{stream_id: sid}} ->
      Scriba.Partitioner.partition(sid, parallelism)
    end

    Broadway.start_link(__MODULE__,
      name: via_tuple(name, version),
      producer: [
        module: {source_module, source_opts},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: parallelism,
          partition_by: partition_by
        ]
      ],
      batchers: [
        default: [
          concurrency: 1,
          batch_size: batch_size,
          batch_timeout: batch_timeout
        ]
      ],
      context: %{
        projection: %{name: name, version: version},
        target_module: target_module,
        target_state: target_state,
        handler: handler,
        repo: repo,
        retry: retry_config
      }
    )
  end

  # Public for testability — exposes the same parse/validate logic
  # start_link/1 uses, so tests can assert configuration shape without
  # spinning up a Broadway pipeline.
  @doc false
  @spec parse_retry_opts(false | nil | true | keyword()) :: %{
          max_attempts: pos_integer(),
          backoff: [non_neg_integer()]
        }
  def parse_retry_opts(false), do: %{max_attempts: 1, backoff: []}
  def parse_retry_opts(nil), do: @default_retry
  def parse_retry_opts(true), do: @default_retry

  def parse_retry_opts(opts) when is_list(opts) do
    max_attempts = Keyword.get(opts, :max_attempts, @default_retry.max_attempts)
    backoff = Keyword.get(opts, :backoff, @default_retry.backoff)

    unless is_integer(max_attempts) and max_attempts >= 1 do
      raise ArgumentError,
            "retry :max_attempts must be a positive integer, got: #{inspect(max_attempts)}"
    end

    unless is_list(backoff) and Enum.all?(backoff, &(is_integer(&1) and &1 >= 0)) do
      raise ArgumentError,
            "retry :backoff must be a list of non-negative integers, got: #{inspect(backoff)}"
    end

    required = max_attempts - 1

    unless length(backoff) >= required do
      raise ArgumentError, """
      Invalid retry config: backoff list must have at least max_attempts - 1 = #{required} entries.
      Got max_attempts: #{max_attempts}, backoff: #{inspect(backoff)} (length #{length(backoff)}).
      Backoff entries are the sleeps between successive attempts; N attempts need N-1 sleeps.
      """
    end

    %{max_attempts: max_attempts, backoff: backoff}
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  def via_tuple(name, version) do
    {:via, Registry, {Scriba.Registry, {:pipeline, name, version}}}
  end

  @doc """
  Returns the pid of this projection's Broadway producer (the source's
  GenStage process), or `nil` if not yet registered.

  Single point of contact with Broadway's internal naming convention.
  Broadway names producers with suffix `"Producer_<index>"` in
  `Broadway.Topology.process_name/3`; with our `concurrency: 1`, the
  one producer is `"Producer_0"`. Our `process_name/2` callback below
  routes that into `Scriba.Internals.Registry`.

  Used by the Coordinator's pause/resume path to deliver
  `Scriba.Source.pause/1` / `resume/1` signals. Returns `nil` during
  the short window between Pipeline supervisor start and Broadway's
  producer registration — Coordinator's :initializing state guards
  this race.

  The `pipeline_naming_smoke_test` integration test asserts this key
  exists after a Pipeline starts, so a future Broadway upgrade that
  changes the naming convention surfaces loudly.
  """
  @spec get_producer_pid(String.t(), pos_integer()) :: pid() | nil
  def get_producer_pid(name, version) do
    case Registry.lookup(Scriba.Internals.Registry, {name, version, "Producer_0"}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @impl Broadway
  def process_name({:via, Registry, {Scriba.Registry, {:pipeline, name, version}}}, suffix) do
    # Broadway-internal processes (producer, processors, batchers, terminator)
    # register in Scriba.Internals.Registry — separate from the public
    # Scriba.Registry so :observer / Registry.lookup callers debugging public
    # addresses don't see a flood of internal entries. Projection identity
    # lives in the tuple key, not in the Registry's atom name, so the BEAM
    # atom table stays bounded regardless of projection count.
    {:via, Registry, {Scriba.Internals.Registry, {name, version, suffix}}}
  end

  @impl Broadway
  def handle_message(_processor, %Message{data: %Scriba.Event{} = event} = msg, ctx) do
    handler_result =
      if already_committed?(event, ctx) do
        # Source-side dedup: the source has redelivered
        # an event whose position is at or below our committed cursor for
        # this stream. The handler effect has already been applied; we
        # return :skip so the target doesn't double-apply, and so
        # handle_batch/4's stream_advances filter (below) keeps the cursor
        # where it is rather than regressing it.
        #
        # Not wrapped in :telemetry.span — no handler ran, so there is no
        # handler latency to report. A distinct :skipped event is emitted
        # instead: skip is the only outcome that leaves no trace anywhere
        # (no read-model row, no dead-letter row, no cursor anomaly), so
        # without counting it nobody — including an operator in production —
        # can close `N == rows + dead_letters + skipped`.
        emit_skipped(ctx, event, :dedup)
        :skip
      else
        meta = %{
          id: event.id,
          stream_id: event.stream_id,
          position: event.position,
          type: event.type,
          metadata: event.metadata,
          occurred_at: event.occurred_at
        }

        # :telemetry.span/3 emits :start/:stop on success and :start/:exception
        # on raise (then re-raises). Wrapping the span in try/rescue
        # intercepts the re-raise here — Broadway never sees it as a
        # failed message — and tags the handler_result so handle_batch/4 can
        # route it to dead-letter. A retry loop wraps around
        # this try/rescue: each retry re-invokes the span (fresh start/stop/
        # exception telemetry per attempt — operators can count :event :start
        # events per event_id to detect retry activity).
        #
        # Pass the same metadata map for both start and stop so both events
        # carry projection / event_type / stream_id / position. On exception,
        # span merges {kind, reason, stacktrace} INTO the start metadata —
        # the exception event's metadata = start_metadata + those three keys.
        span_metadata = %{
          projection: ctx.projection,
          event_type: event.type,
          stream_id: event.stream_id,
          position: event.position
        }

        handler_call = fn ->
          try do
            :telemetry.span(
              [:scriba, :projection, :event],
              span_metadata,
              fn ->
                result = ctx.handler.handle(event.data, meta)
                {result, span_metadata}
              end
            )
          rescue
            # Internal tag shape — never returned by user handlers, only
            # produced here. Matches `Scriba.DeadLetter.normalize_error/1`'s
            # `{:exception, exception, stacktrace}` 3-tuple clause.
            exception ->
              {:exception, exception, __STACKTRACE__}
          end
        end

        case run_with_retries(handler_call, ctx.retry) do
          :skip ->
            # The handler declined this event. Distinguished from :dedup by
            # metadata, because they answer different questions: "this
            # projection does not care about this event type" versus "this
            # event was already applied and was redelivered".
            emit_skipped(ctx, event, :handler)
            :skip

          result ->
            result
        end
      end

    Message.put_data(msg, %{event: event, handler_result: handler_result})
  end

  defp emit_skipped(ctx, event, reason) do
    :telemetry.execute(
      [:scriba, :projection, :event, :skipped],
      %{system_time: System.system_time()},
      %{
        projection: ctx.projection,
        reason: reason,
        event_type: event.type,
        stream_id: event.stream_id,
        position: event.position
      }
    )
  end

  # Retry loop — invokes handler_call up to max_attempts times, sleeping
  # the backoff schedule between attempts on a failure-shape result
  # (`{:error, _}` or the internal `{:exception, _, _}` tag).
  #
  # Why Process.sleep inside handle_message: Broadway processors don't
  # expose a primitive for "delay re-execution of this message." Sleeping
  # in handle_message blocks ONLY this processor — Broadway's per-partition
  # processor model means other partitions continue independently. The
  # sleep does not block the batcher; messages from other processors
  # continue feeding it, batch_timeout fires normally. The retried event
  # will land in a later batch than its natural siblings — a throughput
  # observation, not a correctness one. Per-stream ordering is preserved
  # within the stuck processor's partition.
  #
  # Pipeline restart during the sleep: processor dies, message is not
  # acked, source re-delivers on Pipeline restart, retry counter resets
  # to 0. Clean reset semantic.
  defp run_with_retries(handler_call, retry_config) do
    do_attempt(handler_call, retry_config, 0)
  end

  defp do_attempt(handler_call, %{max_attempts: max} = retry_config, attempt) do
    result = handler_call.()

    cond do
      success_shape?(result) ->
        result

      attempt + 1 < max ->
        # Failure with retries remaining. Sleep the configured backoff,
        # then re-invoke the handler. Each invocation fires its own
        # :telemetry.span — start/stop/exception telemetry per attempt.
        Process.sleep(Enum.at(retry_config.backoff, attempt))
        do_attempt(handler_call, retry_config, attempt + 1)

      true ->
        # Exhausted. Return the final failure result unchanged — the
        # handle_batch partitioning routes it to dead-letter
        # with the original error_kind. No "retry_exhausted" wrapper.
        result
    end
  end

  # Cache-first lookup; falls back to Postgres if `:repo` is configured
  # (Ecto target). Test target users have repo: nil — dedup is bounded to a
  # single Coordinator lifetime. On Coordinator restart the cache is wiped
  # (init_cache in init/1) and the Test source re-yields from zero, so
  # cross-restart dedup requires `:repo` to preload the cursor from
  # Postgres. The real-Postgres property tests will exercise that
  # path.
  defp already_committed?(%Scriba.Event{} = event, ctx) do
    case Scriba.Position.cache_get(
           ctx.projection.name,
           ctx.projection.version,
           event.stream_id,
           repo: ctx.repo
         ) do
      {:ok, committed} -> event.position <= committed
      :error -> false
    end
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, ctx) do
    # Partition into success-shape and failure-shape results. Failure-shape
    # is when the handler returned {:error, _} OR the engine caught a
    # handler raise and tagged it {:exception, exception, stacktrace}. Those
    # go to dead-letter; the rest get their normal Multi step.
    {bad_messages, candidate_messages} =
      Enum.split_with(messages, fn msg ->
        dead_letter?(msg.data.handler_result, ctx.target_module)
      end)

    # Second pass: {:multi, _} operation names must be unique across the whole
    # batch, which is a property of the batch rather than of any one result, so
    # dead_letter?/2 cannot see it.
    {good_messages, collided_messages} = partition_multi_collisions(candidate_messages)

    good_events = Enum.map(good_messages, & &1.data.event)
    good_results = Enum.map(good_messages, & &1.data.handler_result)

    dead_letters =
      Enum.map(bad_messages, fn msg -> {msg.data.event, msg.data.handler_result} end) ++
        Enum.map(collided_messages, fn {msg, keys} ->
          {msg.data.event, {:multi_key_collision, keys}}
        end)

    # stream_advances includes events whose handler returned non-:skip —
    # both success-shape AND dead-lettered results. Per architecture §9.2:
    # dead-lettering advances the cursor past the failed event so the
    # projection doesn't get stuck. Skipped events (dedup-induced OR user
    # :skip) leave the cursor alone — critical for the dedup case where a
    # redelivered batch of below-cursor events must not regress the cursor
    # via Position.multi/5's unconditional ON CONFLICT update.
    stream_advances =
      messages
      |> Enum.reject(fn msg -> msg.data.handler_result == :skip end)
      |> Enum.map(& &1.data.event)
      |> Enum.group_by(& &1.stream_id)
      |> Map.new(fn {sid, evts} ->
        {sid, evts |> Enum.map(& &1.position) |> Enum.max()}
      end)

    # Measure target.apply_batch duration manually rather than via
    # :telemetry.span — we only want a :stop event on the success branch
    # (the Multi committed). Batch-failure observability would be a
    # separate [:scriba, :projection, :batch, :exception] event if/when
    # operationally needed; not in scope for v0.1.
    start_time = System.monotonic_time()

    case ctx.target_module.apply_batch(
           good_events,
           good_results,
           ctx.projection,
           stream_advances,
           dead_letters,
           ctx.target_state
         ) do
      {:ok, _state} ->
        # Postgres-first, ETS-after. The Multi has already committed durably;
        # these cache_put calls are a hot-read optimization for Scriba.info/2
        # and source-side dedup. The window where ETS lags behind Postgres is
        # bounded by Coordinator lifetime: Coordinator.init/1 calls
        # Position.init_cache, which preloads from Postgres when `:repo` is
        # configured (mandatory for Scriba.Target.Ecto via
        # Position.resolve_repo/2). Test target users have no `:repo`; their
        # cache only-ever-reflects state since the current Coordinator started.
        Enum.each(stream_advances, fn {sid, pos} ->
          Scriba.Position.cache_put(ctx.projection.name, ctx.projection.version, sid, pos)
        end)

        # A clean commit clears both the transient backoff escalation and the
        # integrity-wipeout streak.
        Scriba.Circuit.reset(ctx.projection.name, ctx.projection.version)

        # batch_size = events Broadway saw in this batch, including those
        # whose handler returned :skip and those that dead-lettered.
        # Skipped/failed events still consumed pipeline capacity, so the
        # operationally useful "what did this batch process" count includes
        # them.
        :telemetry.execute(
          [:scriba, :projection, :batch, :stop],
          %{duration: System.monotonic_time() - start_time, batch_size: length(messages)},
          %{projection: ctx.projection}
        )

        # One :dead_letter event per dead-lettered event, emitted AFTER the
        # Multi commits — otherwise we'd emit telemetry for rows that didn't
        # actually persist. Metadata shape per architecture §9.3.
        Enum.each(dead_letters, fn {event, error} ->
          :telemetry.execute(
            [:scriba, :projection, :dead_letter],
            %{system_time: System.system_time()},
            %{
              projection: ctx.projection,
              position: event.position,
              stream_id: event.stream_id,
              event_type: event.type,
              error_kind: error_kind(error)
            }
          )
        end)

        messages

      {:error, reason, _state} ->
        handle_commit_failure(reason, messages, good_messages, dead_letters, ctx, start_time)
    end
  end

  # A batch did not commit. Which response terminates depends on WHY, and
  # Scriba.Failure reads that from SQLSTATE rather than guessing.
  #
  # Transient failures replay the batch whole — cheap, and re-applying
  # event-by-event against a database under pressure just multiplies the load
  # that caused the failure.
  #
  # Integrity and structural failures are deterministic: replaying reproduces
  # them exactly, forever. Those go to the per-event pass, which isolates the
  # offending event so the rest of the batch can make progress.
  defp handle_commit_failure(reason, messages, good_messages, dead_letters, ctx, start_time) do
    case Scriba.Failure.classify(reason) do
      :transient ->
        # Carry an escalating delay to the producer so it waits before dying.
        # Nothing else throttles this path: without it the producer crashes as
        # fast as batches form and exhausts the supervisor's restart budget in
        # seconds, no matter how large that budget is.
        delay = Scriba.Circuit.record_transient(ctx.projection.name, ctx.projection.version)
        Enum.map(messages, &Message.failed(&1, {:scriba_replay, reason, delay}))

      _deterministic ->
        fallback_per_event(reason, messages, good_messages, dead_letters, ctx, start_time)
    end
  end

  # Re-applies the batch one transaction per event, in per-stream position
  # order, so a single bad event can be dead-lettered instead of poisoning
  # every event that shares its batch.
  #
  # Two rules make this safe, and both are load-bearing:
  #
  #   1. **A stream stops at its first unresolved event.** Not "cap the
  #      cursor" — stop applying. If event 5 fails transiently and 6 commits,
  #      the cursor cannot advance past 5, so replay redelivers 6 as well and
  #      double-applies it. Stopping also preserves per-stream ordering, which
  #      applying 6 before 5 would violate outright.
  #
  #   2. **If anything is left unresolved, nothing is acknowledged.** Acks are
  #      prefix-acks, and batches interleave streams: acknowledging a resolved
  #      event in stream A would implicitly acknowledge an earlier unresolved
  #      event in stream B and lose it. So a partial pass still fails the whole
  #      batch and replays — but the work it committed is durable, and
  #      source-side dedup filters it on the way back.
  #
  # The win is not partial acknowledgement. It is that an integrity failure
  # ends as a dead letter and forward progress, instead of an infinite loop.
  defp fallback_per_event(batch_reason, messages, good_messages, dead_letters, ctx, start_time) do
    units =
      Enum.map(good_messages, fn msg -> {msg.data.event, {:apply, msg.data.handler_result}} end) ++
        Enum.map(dead_letters, fn {event, error} -> {event, {:dead_letter, error}} end)

    acc0 = %{resolved: [], unresolved: 0, halt: nil, attempted: 0, committed: 0, integrity: 0}

    outcome =
      units
      |> Enum.group_by(fn {event, _unit} -> event.stream_id end)
      |> Enum.reduce(acc0, fn {_sid, stream_units}, acc ->
        stream_units
        |> Enum.sort_by(fn {event, _unit} -> event.position end)
        |> apply_stream_units(ctx, acc)
      end)

    cond do
      outcome.halt != nil ->
        halt_batch(outcome.halt, messages, ctx)

      # Blast radius. SQLSTATE says a failure is deterministic; it cannot say
      # how many events share the defect. Every attempted write failing on
      # integrity grounds is not one poison row — it is a schema the handler
      # no longer matches, and dead-lettering it event by event would drain
      # the stream into scriba_dead_letters and report a caught-up projection
      # over an empty read model.
      wipeout?(outcome) and
          Scriba.Circuit.record_wipeout(
            ctx.projection.name,
            ctx.projection.version,
            outcome.attempted
          ) == :halt ->
        halt_batch({:integrity_wipeout, outcome.attempted}, messages, ctx)

      outcome.unresolved > 0 ->
        # Committed work stands; the batch replays and dedup filters it.
        delay = Scriba.Circuit.record_transient(ctx.projection.name, ctx.projection.version)
        Enum.map(messages, &Message.failed(&1, {:scriba_replay, batch_reason, delay}))

      true ->
        # Everything resolved — committed or dead-lettered. The poison event
        # is out of the way and the projection moves on.
        if outcome.committed > 0, do: Scriba.Circuit.reset(ctx.projection.name, ctx.projection.version)
        emit_batch_stop(ctx, start_time, length(messages))
        Enum.each(outcome.resolved, &emit_dead_letter(&1, ctx))
        messages
    end
  end

  defp wipeout?(%{attempted: attempted, committed: 0, integrity: integrity})
       when attempted > 0 and attempted == integrity,
       do: true

  defp wipeout?(_outcome), do: false

  defp apply_stream_units(_units, _ctx, %{halt: halt} = acc) when halt != nil, do: acc

  defp apply_stream_units(units, ctx, acc) do
    Enum.reduce_while(units, acc, fn {event, unit}, acc ->
      attempted? = match?({:apply, result} when result != :skip, unit)
      acc = if attempted?, do: %{acc | attempted: acc.attempted + 1}, else: acc

      case apply_unit(event, unit, ctx) do
        :skipped ->
          {:cont, acc}

        {:committed, _} ->
          {:cont, %{acc | committed: acc.committed + 1}}

        {:integrity_dead_letter, dead_letter} ->
          {:cont,
           %{
             acc
             | integrity: acc.integrity + 1,
               resolved: acc.resolved ++ List.wrap(dead_letter)
           }}

        {:resolved, dead_letter} ->
          {:cont, %{acc | resolved: acc.resolved ++ List.wrap(dead_letter)}}

        :unresolved ->
          # Stop this stream here — see rule 1 above. Remaining units on this
          # stream stay unapplied and replay in order.
          {:halt, %{acc | unresolved: acc.unresolved + 1}}

        {:halt, reason} ->
          {:halt, %{acc | halt: reason}}
      end
    end)
  end

  defp apply_unit(_event, {:apply, :skip}, _ctx), do: :skipped

  defp apply_unit(event, {:apply, result}, ctx) do
    case commit_one(ctx, [event], [result], event, []) do
      {:ok, _state} ->
        cache_put_one(ctx, event)
        {:committed, nil}

      {:error, reason, _state} ->
        resolve_commit_failure(event, reason, ctx)
    end
  end

  defp apply_unit(event, {:dead_letter, error}, ctx) do
    # A first-pass dead letter. Its row rolled back with the batch, so it has
    # to be written again here — otherwise every handler failure and multi-key
    # collision in the batch vanishes silently, which is the same discard bug
    # in a new place.
    dead_letter_one(event, error, ctx)
  end

  defp resolve_commit_failure(event, reason, ctx) do
    case Scriba.Failure.classify(reason) do
      # Deterministic and specific to this event: record it and move on.
      # Counted separately, because "all of them" means something different —
      # see Scriba.Circuit.record_wipeout/3.
      :integrity -> integrity_dead_letter(event, reason, ctx)
      :transient -> :unresolved
      :structural -> {:halt, reason}
    end
  end

  defp integrity_dead_letter(event, reason, ctx) do
    case dead_letter_one(event, {:commit_error, reason}, ctx) do
      {:resolved, dead_letter} -> {:integrity_dead_letter, dead_letter}
      other -> other
    end
  end

  defp dead_letter_one(event, error, ctx) do
    case commit_one(ctx, [], [], event, [{event, error}]) do
      {:ok, _state} ->
        cache_put_one(ctx, event)
        {:resolved, {event, error}}

      # Cannot even record the failure — the schema or connection is beyond
      # what this pass can resolve.
      {:error, reason, _state} ->
        {:halt, reason}
    end
  end

  defp commit_one(ctx, events, results, event, dead_letters) do
    ctx.target_module.apply_batch(
      events,
      results,
      ctx.projection,
      %{event.stream_id => event.position},
      dead_letters,
      ctx.target_state
    )
  end

  defp cache_put_one(ctx, event) do
    Scriba.Position.cache_put(
      ctx.projection.name,
      ctx.projection.version,
      event.stream_id,
      event.position
    )
  end

  # Structural failure: the schema or permissions do not match the code.
  # Dead-lettering would destroy a projection's worth of events over a
  # fixable deploy-ordering mistake; replaying loops forever. Halt — but
  # announce it, because a silent stall is the failure this library had.
  defp halt_batch(reason, messages, ctx) do
    # Make it queryable, not just observable at the instant it happens.
    # Telemetry and the log below are edge-triggered: an operator who was not
    # subscribed when this fired would otherwise see `Scriba.info/2` report
    # `:running` for a projection that will never move again.
    Coordinator.halt(ctx.projection.name, ctx.projection.version, reason)

    :telemetry.execute(
      [:scriba, :projection, :halted],
      %{system_time: System.system_time()},
      %{projection: ctx.projection, reason: reason, failure: Scriba.Failure.label(reason)}
    )

    Logger.error("""
    Scriba projection #{ctx.projection.name} v#{ctx.projection.version} halted.

    A batch failed with a structural error, which neither replaying nor
    dead-lettering can resolve: #{Scriba.Failure.label(reason)}

    #{inspect(reason)}

    This usually means handler code was deployed ahead of its migration, or the
    projection lacks a privilege it needs. The projection is not acknowledging
    events and will make no progress until it is fixed and restarted.
    """)

    Enum.map(messages, &Message.failed(&1, {:scriba_halt, reason}))
  end

  defp emit_batch_stop(ctx, start_time, batch_size) do
    :telemetry.execute(
      [:scriba, :projection, :batch, :stop],
      %{duration: System.monotonic_time() - start_time, batch_size: batch_size},
      %{projection: ctx.projection}
    )
  end

  defp emit_dead_letter({event, error}, ctx) do
    :telemetry.execute(
      [:scriba, :projection, :dead_letter],
      %{system_time: System.system_time()},
      %{
        projection: ctx.projection,
        position: event.position,
        stream_id: event.stream_id,
        event_type: event.type,
        error_kind: error_kind(error)
      }
    )
  end

  # failure_shape?/1 — the two results that are unambiguously failures
  # regardless of target: the explicit `{:error, _}` return from §4.2 and the
  # engine-internal `{:exception, _, _}` tag produced by handle_message's
  # try/rescue. These are what the retry layer retries.
  defp failure_shape?({:error, _}), do: true
  defp failure_shape?({:exception, _, _}), do: true
  defp failure_shape?(_), do: false

  defp success_shape?(result), do: not failure_shape?(result)

  # dead_letter?/2 — what handle_batch/4 partitions on. Broader than
  # failure_shape?/1: a result the target cannot apply is also a dead letter,
  # even though it is neither an error nor an exception.
  #
  # Asking the target rather than whitelisting §4.2 here is deliberate. The
  # six shapes are Scriba.Target.Ecto's vocabulary, not the engine's —
  # Scriba.Target.Test accepts any non-:skip result, and v0.4 targets will
  # define their own. A whitelist in the Pipeline would foreclose that.
  #
  # Why this check exists at all: without it a malformed return reaches
  # apply_batch/6 and raises during Multi assembly, *outside* that function's
  # `case repo.transaction(...)`. Broadway fails the whole batch and the
  # source correctly refuses to acknowledge it — but the cause is one
  # deterministic bad return, so every redelivery reproduces it exactly. The
  # projection crash-loops forever on a single event, with no dead letter and
  # no progress. Rejecting it here makes it a per-event dead letter with the
  # cursor advancing past it, which is what a user bug in one handler clause
  # deserves.
  defp dead_letter?(result, target_module) do
    failure_shape?(result) or not target_accepts?(target_module, result)
  end

  # Ecto.Multi raises on duplicate operation names, and Scriba merges every
  # event's {:multi, _} into ONE batch Multi — so two events naming an
  # operation the same way collide. Under commanded_ecto_projections' one
  # transaction per event that was impossible, and its docs used a static
  # atom key, so migrated projectors are the likely source.
  #
  # Left undetected this is the worst remaining failure mode: merges resolve
  # lazily, so the raise happens inside Repo.transaction/1, outside
  # apply_batch/6's case. Broadway fails the batch, the source refuses to ack
  # it (correctly), it is redelivered, and the same two events collide again
  # — deterministically, forever, with no dead letter and no progress.
  #
  # Detected here instead, the first claim on a name wins and later claimants
  # are dead-lettered individually. That keeps the batch moving and names the
  # offending event and key, which a transaction-time raise cannot do (it
  # knows the key, not which event produced it).
  #
  # Namespacing user keys was the alternative and was rejected: rewriting
  # operation names would break any Ecto.Multi.run/3 callback that reads a
  # prior step out of `changes` by its declared name.
  defp partition_multi_collisions(messages) do
    {kept, collided, _claimed} =
      Enum.reduce(messages, {[], [], MapSet.new()}, fn msg, {kept, collided, claimed} ->
        names = multi_op_names(msg.data.handler_result)

        case Enum.filter(names, &(MapSet.member?(claimed, &1) or reserved_key?(&1))) do
          [] ->
            {[msg | kept], collided, Enum.into(names, claimed)}

          dupes ->
            {kept, [{msg, dupes} | collided], claimed}
        end
      end)

    {Enum.reverse(kept), Enum.reverse(collided)}
  end

  # Public API only — `to_list/1` rather than the struct's `:names` field, to
  # avoid depending on Ecto internals.
  #
  # The `:merge` filter is required, not cosmetic: Ecto.Multi.merge/2 appends
  # `{:merge, _}` straight to :operations without registering a name, so
  # to_list/1 reports every merge under the pseudo-name `:merge`. Without the
  # reject, two events that each merge internally would look like a collision.
  # The cost is that an operation a user genuinely named `:merge` is not
  # checked — it then behaves as it does today.
  defp multi_op_names({:multi, %Ecto.Multi{} = user_multi}) do
    user_multi
    |> Ecto.Multi.to_list()
    |> Enum.map(&elem(&1, 0))
    |> Enum.reject(&(&1 == :merge))
  end

  defp multi_op_names(_other), do: []

  # Scriba's own step keys. A user multi claiming one of these collides with
  # the engine rather than with a peer event.
  defp reserved_key?({:scriba_event, _}), do: true
  defp reserved_key?({:scriba_position, _}), do: true
  defp reserved_key?({:scriba_dead_letter, _}), do: true
  defp reserved_key?(_other), do: false

  defp target_accepts?(target_module, result) do
    if function_exported?(target_module, :valid_result?, 1) do
      target_module.valid_result?(result)
    else
      true
    end
  end

  # error_kind/1 — matches DeadLetter.normalize_error/1's kind output so
  # telemetry metadata and dead-letter rows agree on the kind label.
  defp error_kind({:error, _}), do: "error"
  defp error_kind({:exception, exception, _}) when is_exception(exception),
    do: exception.__struct__ |> Atom.to_string()

  defp error_kind({:multi_key_collision, _keys}), do: "multi_key_collision"

  # An integrity violation isolated by the per-event fallback. Labelled with
  # the SQLSTATE so the dead-letter table says "23505 (unique_violation)"
  # rather than something an operator has to go and decode.
  defp error_kind({:commit_error, reason}), do: "commit:" <> Scriba.Failure.label(reason)

  # Reached when a handler returned something outside §4.2's six shapes.
  # Named rather than lumped into "unknown" so the dead-letter table
  # distinguishes "my handler failed" from "my handler is wrong".
  defp error_kind(_), do: "invalid_return"
end
