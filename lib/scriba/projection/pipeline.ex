defmodule Scriba.Projection.Pipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    version = Keyword.fetch!(opts, :version)

    {source_module, source_opts} = Keyword.fetch!(opts, :source)
    {target_module, target_opts} = Keyword.fetch!(opts, :target)
    parallelism = Keyword.fetch!(opts, :parallelism)
    handler = Keyword.fetch!(opts, :handler)
    batch_size = Keyword.get(opts, :batch_size, 50)
    batch_timeout = Keyword.get(opts, :batch_timeout, 100)

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
        repo: repo
      }
    )
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
        # route it to dead-letter. will wrap a retry loop
        # around this try/rescue: same internal tag shape, retry sits between
        # the catch and the batch.
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

    Message.put_data(msg, %{event: event, handler_result: handler_result})
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
