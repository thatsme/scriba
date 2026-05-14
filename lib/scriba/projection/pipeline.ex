defmodule Scriba.Projection.Pipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  # Default retry policy per architecture §9.1 and :
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
        # Skipped events are NOT wrapped in :telemetry.span — they didn't
        # invoke the user handler, so there is no handler latency to emit.
        # If dedup visibility becomes operationally interesting, add a
        # separate [:scriba, :projection, :event, :skipped] event.
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
        # on raise (then re-raises). Wrapping the span in try/rescue (
        # item 2) intercepts the re-raise here — Broadway never sees it as a
        # failed message — and tags the handler_result so handle_batch/4 can
        # route it to dead-letter. wraps a retry loop around
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

        run_with_retries(handler_call, ctx.retry)
      end

    Message.put_data(msg, %{event: event, handler_result: handler_result})
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
        # Exhausted. Return the final failure result unchanged — 
        # item 2's handle_batch partitioning routes it to dead-letter
        # with the original error_kind. No "retry_exhausted" wrapper.
        result
    end
  end

  # Cache-first lookup; falls back to Postgres if `:repo` is configured
  # (Ecto target). Test target users have repo: nil — dedup is bounded to a
  # single Coordinator lifetime. On Coordinator restart the cache is wiped
  # (init_cache in init/1) and the Test source re-yields from zero, so
  # cross-restart dedup requires `:repo` to preload the cursor from
  # Postgres. 's real-Postgres property tests will exercise that
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
    #: handler returned {:error, _} OR the engine caught a
    # handler raise and tagged it {:exception, exception, stacktrace}. Those
    # go to dead-letter; the rest get their normal Multi step.
    {good_messages, bad_messages} =
      Enum.split_with(messages, fn msg -> success_shape?(msg.data.handler_result) end)

    good_events = Enum.map(good_messages, & &1.data.event)
    good_results = Enum.map(good_messages, & &1.data.handler_result)
    dead_letters = Enum.map(bad_messages, fn msg -> {msg.data.event, msg.data.handler_result} end)

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
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  # success_shape?/1 — false for handler returns that route to dead-letter:
  # `{:error, _}` (explicit failure return per §4.2) and the engine-internal
  # `{:exception, _, _}` tag produced by handle_message's try/rescue. Every
  # other shape flows to the Target's normal apply_batch path. Garbage
  # handler returns (not in §4.2's six shapes) are not dead-lettered — they
  # surface as a loud failure in the Target (FunctionClauseError in the
  # Ecto target). That's the right place for "user wrote a broken handler"
  # diagnostics; dead-lettering would swallow it.
  defp success_shape?({:error, _}), do: false
  defp success_shape?({:exception, _, _}), do: false
  defp success_shape?(_), do: true

  # error_kind/1 — matches DeadLetter.normalize_error/1's kind output so
  # telemetry metadata and dead-letter rows agree on the kind label.
  defp error_kind({:error, _}), do: "error"
  defp error_kind({:exception, exception, _}) when is_exception(exception),
    do: exception.__struct__ |> Atom.to_string()
  defp error_kind(_), do: "unknown"
end
