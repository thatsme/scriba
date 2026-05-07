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

    Broadway.start_link(__MODULE__,
      name: via_tuple(name, version),
      producer: [
        module: {source_module, source_opts},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: parallelism,
          partition_by: &partition_by_stream/1
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
        handler: handler
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
    :"scriba_pipeline_#{name}_v#{version}_#{suffix}"
  end

  defp partition_by_stream(%Message{data: %Scriba.Event{stream_id: sid}}) do
    :erlang.phash2(sid)
  end

  @impl Broadway
  def handle_message(_processor, %Message{data: %Scriba.Event{} = event} = msg, ctx) do
    meta = %{
      stream_id: event.stream_id,
      position: event.position,
      type: event.type,
      metadata: event.metadata,
      occurred_at: event.occurred_at
    }

    handler_result = ctx.handler.handle(event.data, meta)

    Message.put_data(msg, %{event: event, handler_result: handler_result})
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, ctx) do
    events = Enum.map(messages, & &1.data.event)
    handler_results = Enum.map(messages, & &1.data.handler_result)

    # Per-stream max position. With Broadway's partition_by(stream_id) above,
    # one stream's events all arrive at the same processor and reach the
    # batcher in source order — so the max position per stream within a
    # batch is monotonic w.r.t. the previous batch's max for that same
    # stream. The global cross-partition reorder bug that motivated the
    # max(current, batch_max) clamp is moot under per-stream cursors.
    stream_advances =
      events
      |> Enum.group_by(& &1.stream_id)
      |> Map.new(fn {sid, evts} ->
        {sid, evts |> Enum.map(& &1.position) |> Enum.max()}
      end)

    case ctx.target_module.apply_batch(
           events,
           handler_results,
           ctx.projection,
           stream_advances,
           ctx.target_state
         ) do
      {:ok, _state} ->
        # Postgres-first, ETS-after. The Multi has already committed durably;
        # these cache_put calls are a hot-read optimization for Scriba.info/2.
        # The window where ETS lags behind Postgres is bounded by Coordinator
        # restart: when :repo is configured (mandatory for Scriba.Target.Ecto;
        # see Coordinator.resolve_repo/2), Position.init_cache preloads from
        # Postgres on entry to :running and the cache catches back up. Test
        # target users have no :repo and no Postgres — for them, the cache is
        # only-ever-correct under the assumption the projection isn't restarted
        # mid-run, which the test harness controls explicitly.
        Enum.each(stream_advances, fn {sid, pos} ->
          Scriba.Position.cache_put(ctx.projection.name, ctx.projection.version, sid, pos)
        end)

        messages

      {:error, reason, _state} ->
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end
end
