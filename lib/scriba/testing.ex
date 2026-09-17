defmodule Scriba.Testing do
  @moduledoc """
  Test a projection's `handle/2` clauses without starting a pipeline.

  A projection is an ordinary module, so its clauses can be called directly —
  but doing that by hand means building the `meta` map yourself, and it stops
  short of the part worth asserting: the read-model rows. These helpers run
  the handler the way the engine does and commit the result through the same
  target, so a test can assert on rows.

      test "a deposit increases the balance" do
        Scriba.Testing.project(MyApp.Projections.Balances, [
          %AccountOpened{account_id: "acc-1"},
          %Deposited{account_id: "acc-1", amount_cents: 500}
        ])

        assert Repo.get(Balance, "acc-1").balance_cents == 500
      end

  Both helpers work inside `Ecto.Adapters.SQL.Sandbox`; nothing here starts a
  process, so no ownership needs to be shared.

  ## What this exercises, and what it does not

  It runs the real handler with the real `meta` map, applies the results
  through the projection's configured target, and commits in one transaction —
  the same `c:Scriba.Target.apply_batch/6` the engine calls, so read-model
  writes, `Ecto.Multi` merging and cursor advances behave as in production.

  It does **not** simulate the surrounding pipeline: no retries, no
  dead-letter rows, no source-side dedup, no partitioning across processors,
  no telemetry. Handler failures are reported back to the caller instead of
  being routed, so a test can assert on them directly. For the routing itself
  — what gets dead-lettered, what halts, what replays — see the engine's own
  suite; reproducing those decisions here would be a second implementation of
  them, and the copy would be the one that drifts.
  """

  alias Scriba.Event

  defmodule Result do
    @moduledoc """
    What `Scriba.Testing.project/3` did.

      * `:committed` — how many events produced a read-model write.
      * `:skipped` — events whose handler returned `:skip`, as
        `{event_data, meta}`.
      * `:failed` — events whose handler returned `{:error, reason}` or
        raised, as `{event_data, reason}`. In a running projection these
        would be retried and then dead-lettered.
      * `:invalid` — handler returns the target cannot apply, as
        `{event_data, result}`. In a running projection these are
        dead-lettered without retry.
    """

    @type t :: %__MODULE__{
            committed: non_neg_integer(),
            skipped: [{term(), map()}],
            failed: [{term(), term()}],
            invalid: [{term(), term()}]
          }

    defstruct committed: 0, skipped: [], failed: [], invalid: []
  end

  @doc """
  Calls one `handle/2` clause and returns its result.

  No database, no target — useful for asserting the shape a handler returns,
  including `:skip` for event types the projection ignores.

      assert :skip = Scriba.Testing.handle(MyProjection, %SomeOtherEvent{})

      assert {:insert, %Balance{account_id: "acc-1"}} =
               Scriba.Testing.handle(MyProjection, %AccountOpened{account_id: "acc-1"})

  `opts` override the `meta` map the handler receives: `:id`, `:stream_id`,
  `:position`, `:type`, `:metadata`, `:occurred_at`. The defaults are the same
  shape the engine builds, so a handler reading `meta.position` works here too.

  A raising handler is returned as `{:exception, exception, stacktrace}`,
  which is what the engine tags it as — it is not re-raised, so a test can
  assert that a handler fails on input it should reject.
  """
  @spec handle(module(), term(), keyword()) :: term()
  def handle(projection, event_data, opts \\ []) when is_atom(projection) do
    {handler, _config} = handler_for(projection)
    meta = meta(event_data, opts, 1)

    invoke(handler, event_data, meta)
  end

  @doc """
  Runs `events` through the projection and commits the results.

  Returns a `Scriba.Testing.Result`. Events are applied in the order given, in a
  single transaction, exactly as one batch would be.

      %Scriba.Testing.Result{committed: 2, failed: []} =
        Scriba.Testing.project(MyProjection, [event_a, event_b])

  Each element is either the event struct, or `{event_struct, opts}` where
  `opts` sets that event's `meta` — most usefully `:stream_id`, since events
  on different streams get independent cursors.

      Scriba.Testing.project(MyProjection, [
        {%Deposited{}, stream_id: "acc-1"},
        {%Deposited{}, stream_id: "acc-2"}
      ])

  ## Options

    * `:repo` — overrides the repo from the projection's target config. Useful
      when the test repo differs from the production one.
    * `:name`, `:version` — the projection identity written to
      `scriba_positions`. Default to the projection's own.
    * `:stream_id` — default stream for events that do not set one
      (default `"scriba-test"`).
    * `:start_position` — position of the first event (default `1`);
      subsequent events increment from there.

  Raises if the target rejects the batch, since that is a failed commit rather
  than a handler outcome the caller can assert on.
  """
  @spec project(module(), [term()], keyword()) :: Result.t()
  def project(projection, events, opts \\ []) when is_atom(projection) and is_list(events) do
    {handler, config} = handler_for(projection)
    {target_module, target_opts} = target_for(config, opts)

    projection_id = %{
      name: Keyword.get(opts, :name, config.name),
      version: Keyword.get(opts, :version, config.version)
    }

    default_stream = Keyword.get(opts, :stream_id, "scriba-test")
    start_position = Keyword.get(opts, :start_position, 1)

    events
    |> Enum.with_index(start_position)
    |> Enum.map(fn {event, position} ->
      {data, event_opts} = split_event(event)
      event_opts = Keyword.put_new(event_opts, :stream_id, default_stream)
      meta = meta(data, event_opts, position)

      {data, meta, invoke(handler, data, meta)}
    end)
    |> commit(target_module, target_opts, projection_id)
  end

  ## Internals

  # An outcome is `{event_data, meta, handler_result}`. Three steps, each
  # nameable: decide what the target can take, describe what happened, apply
  # it.
  defp commit(outcomes, target_module, target_opts, projection_id) do
    {appliable, rejected} = Enum.split_with(outcomes, &appliable?(&1, target_module))

    result = summarise(appliable, rejected)

    case apply_to_target(appliable, target_module, target_opts, projection_id) do
      :ok -> result
      {:error, reason} -> raise commit_failure_message(reason)
    end
  end

  defp summarise(appliable, rejected) do
    %{classify(rejected) | skipped: skipped(appliable), committed: count_committed(appliable)}
  end

  defp apply_to_target(appliable, target_module, target_opts, projection_id) do
    events = Enum.map(appliable, fn {data, meta, _} -> event_struct(data, meta) end)
    results = Enum.map(appliable, fn {_, _, result} -> result end)
    advances = stream_advances(appliable)

    {:ok, state} = target_module.init(target_opts)

    case target_module.apply_batch(events, results, projection_id, advances, [], state) do
      {:ok, _state} -> :ok
      {:error, reason, _state} -> {:error, reason}
    end
  end

  # `:skip` is an applicable result — the target has a clause for it — so
  # skipped events are found among those, not among the rejected ones.
  defp skipped(appliable) do
    for {data, meta, :skip} <- appliable, do: {data, meta}
  end

  defp count_committed(appliable) do
    Enum.count(appliable, fn {_, _, result} -> result != :skip end)
  end

  # The rule the pipeline applies: a `:skip` leaves its stream's cursor where
  # it is, whatever the reason for the skip.
  defp stream_advances(appliable) do
    appliable
    |> Enum.reject(fn {_, _, result} -> result == :skip end)
    |> Enum.group_by(fn {_, meta, _} -> meta.stream_id end, fn {_, meta, _} -> meta.position end)
    |> Map.new(fn {stream_id, positions} -> {stream_id, Enum.max(positions)} end)
  end

  defp commit_failure_message(reason) do
    """
    Scriba.Testing.project/3 could not commit the batch.

        #{inspect(reason)}

    This is a target failure, not a handler outcome — the read model schema,
    the repo or the migration is the thing to look at. Scriba's engine would
    classify this and either replay, dead-letter or halt.
    """
  end

  defp classify(rejected) do
    Enum.reduce(rejected, %Result{}, fn {data, _meta, result}, acc ->
      case result do
        {:error, reason} -> %{acc | failed: acc.failed ++ [{data, reason}]}
        {:exception, exception, _stack} -> %{acc | failed: acc.failed ++ [{data, exception}]}
        other -> %{acc | invalid: acc.invalid ++ [{data, other}]}
      end
    end)
  end

  # :skip is appliable — it produces no Multi op but must still be counted, and
  # the target's own valid_result?/1 says so.
  defp appliable?({_data, _meta, result}, target_module) do
    # Code.ensure_loaded? first: function_exported?/3 answers false for a
    # module that simply has not been loaded yet, which made this check
    # depend on whether something else had happened to call the target
    # already. A validation that silently turns itself off is worse than no
    # validation.
    Code.ensure_loaded?(target_module)

    if function_exported?(target_module, :valid_result?, 1) do
      target_module.valid_result?(result)
    else
      not match?({:error, _}, result) and not match?({:exception, _, _}, result)
    end
  end

  defp invoke(handler, event_data, meta) do
    handler.handle(event_data, meta)
  rescue
    exception -> {:exception, exception, __STACKTRACE__}
  end

  defp split_event({data, opts}) when is_list(opts), do: {data, opts}
  defp split_event(data), do: {data, []}

  defp meta(event_data, opts, position) do
    %{
      id: Keyword.get(opts, :id, "scriba-test-#{position}"),
      stream_id: Keyword.get(opts, :stream_id, "scriba-test"),
      position: Keyword.get(opts, :position, position),
      type: Keyword.get(opts, :type, type_of(event_data)),
      metadata: Keyword.get(opts, :metadata, %{}),
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
    }
  end

  defp event_struct(data, meta) do
    %Event{
      id: meta.id,
      stream_id: meta.stream_id,
      type: meta.type,
      data: data,
      metadata: meta.metadata,
      position: meta.position,
      occurred_at: meta.occurred_at
    }
  end

  defp type_of(%module{}), do: Atom.to_string(module)
  defp type_of(_other), do: "unknown"

  defp handler_for(projection) do
    config = config_for(projection)
    {Map.get(config, :handler, projection), config}
  end

  defp config_for(projection) do
    if function_exported?(projection, :__scriba_config__, 0) do
      projection.__scriba_config__()
    else
      Code.ensure_loaded(projection)

      if function_exported?(projection, :__scriba_config__, 0) do
        projection.__scriba_config__()
      else
        raise ArgumentError, """
        #{inspect(projection)} is not a Scriba projection — it does not define
        __scriba_config__/0. Did you mean a module that has
        `use Scriba.Projection`?
        """
      end
    end
  end

  defp target_for(config, opts) do
    {target_module, target_opts} = config.target

    target_opts =
      case Keyword.fetch(opts, :repo) do
        {:ok, repo} -> Keyword.put(target_opts, :repo, repo)
        :error -> target_opts
      end

    {target_module, target_opts}
  end
end
