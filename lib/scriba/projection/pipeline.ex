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

        # :telemetry.span/3 emits start/stop on success and start/exception
        # on raise (then re-raises — Broadway still sees the failure and
        # marks the message). (retry) and 3 (dead-letter)
        # will wrap OUTSIDE this span to intercept before Broadway's default
        # failure path.
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

        :telemetry.span(
          [:scriba, :projection, :event],
          span_metadata,
          fn ->
            result = ctx.handler.handle(event.data, meta)
            {result, span_metadata}
          end
        )
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
    events = Enum.map(messages, & &1.data.event)
    handler_results = Enum.map(messages, & &1.data.handler_result)

    # stream_advances only includes events whose handler returned a non-:skip
    # result — i.e. events that were actually applied to the read model.
    # Skipped events (dedup-induced OR user-handler :skip) leave the cursor
    # alone. Crucially, this prevents the dedup case from regressing the
    # cursor: a redelivered batch of events all below the current cursor
    # would otherwise overwrite it via Position.multi/5's unconditional
    # `ON CONFLICT SET position = EXCLUDED.position`. A stream that contains
    # only :skip events in this batch is absent from stream_advances —
    # correctly, since there's nothing to advance.
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
           events,
           handler_results,
           ctx.projection,
           stream_advances,
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
        # whose handler returned :skip. Skipped events still consumed
        # pipeline capacity, so the operationally useful "what did this
        # batch process" count includes them.
        :telemetry.execute(
          [:scriba, :projection, :batch, :stop],
          %{duration: System.monotonic_time() - start_time, batch_size: length(messages)},
          %{projection: ctx.projection}
        )

        messages

      {:error, reason, _state} ->
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end
end
