defmodule Scriba.Projection do
  @moduledoc """
  Behaviour + `use` macro for the user-facing projection module.

  See architecture doc §4.1 for the canonical example. The shortest form:

      defmodule MyApp.Projections.Orders do
        use Scriba.Projection,
          name: "orders",
          source: {Scriba.Source.Commanded, application: MyApp.CommandedApp},
          target: {Scriba.Target.Ecto, repo: MyApp.Repo},
          parallelism: 16

        def handle(%OrderPlaced{} = event, _meta) do
          {:insert, %OrderReadModel{id: event.order_id, ...}}
        end

        def handle(_event, _meta), do: :skip
      end

  Then:

      {:ok, _pid} = Scriba.start_projection(MyApp.Projections.Orders)

  ## Required options

    * `:name` — projection's logical identity. String. Per architecture §5,
      do NOT encode version in the name (e.g. `"orders_v1"`); use the
      separate `:version` option for that. The macro emits a compile-time
      warning when `:name` matches `_v<integer>$`.
    * `:source` — `{module, opts}` tuple. The module must implement
      `Scriba.Source` (and Broadway's `Producer`).
    * `:target` — `{module, opts}` tuple. The module must implement
      `Scriba.Target`.
    * `:parallelism` — positive integer. Number of Broadway processors;
      also the modulus for the per-stream partition hash. **No default** —
      this is a real performance decision that should be deliberate.

  ## Optional options (with defaults)

    * `:version` — `1`. Bump to run a new projection side-by-side with the
      old one (two modules, two version integers).
    * `:partition_by` — `:stream_id`. **Only `:stream_id` is supported in
      v0.1.** Custom partitioners are post-v0.1; passing anything else
      raises at compile time.
    * `:handler` — `__MODULE__`. The module whose `handle/2` clauses the
      engine calls. Defaults to the module being `use`d.
    * `:batch_size`, `:batch_timeout`, `:retry` — pass through to Pipeline
      and 's retry layer respectively. See
      `Scriba.Projection.Pipeline` and `SCRIBA_ARCHITECTURE.md` §9.1 for
      defaults.

  ## `handle/2` callback contract

  See architecture §4.2 for the six valid return shapes:

    * `:skip`
    * `{:insert, schema_struct}`
    * `{:update, schema_module, filter, [set: changes]}`
    * `{:delete, schema_module, filter}`
    * `{:multi, %Ecto.Multi{}}`
    * `{:error, reason}` (routes to dead-letter)

  Raising is also valid and treated like `{:error, exception}` (with the
  exception struct and stacktrace preserved for the dead-letter row).
  Retries apply to both shapes unless `retry: false`.
  """

  @doc """
  Handles a single event. The engine calls this once per event in
  per-stream order. See module doc for the valid return shapes.
  """
  @callback handle(event_data :: term(), meta :: map()) :: term()

  @doc false
  defmacro __using__(opts) do
    # Validate and normalize at compile time. The macro raises here (not
    # at runtime) so projection-module bugs surface during mix compile
    # rather than mid-flight in production.
    {config, caller_env} = build_config(opts, __CALLER__)

    quote do
      @behaviour Scriba.Projection

      @doc """
      Returns this projection's compile-time configuration map. Generated
      by `use Scriba.Projection`. Used by `Scriba.start_projection/1` and
      the module-form dispatch in `Scriba.pause/1` etc.
      """
      @spec __scriba_config__() :: map()
      def __scriba_config__, do: unquote(Macro.escape(config))

      # Caller's env captured for error messages on misuse — currently
      # unused, but kept for future diagnostics.
      _ = unquote(Macro.escape(caller_env))
    end
  end

  ## Compile-time validation + defaults

  @valid_keys [
    :name,
    :version,
    :source,
    :target,
    :parallelism,
    :handler,
    :partition_by,
    :batch_size,
    :batch_timeout,
    :retry
  ]

  defp build_config(opts, caller_env) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, """
      use Scriba.Projection expects a keyword list of options, got: #{inspect(opts)}
      """
    end

    # Resolve aliases. Module references like `Scriba.Test.Source` arrive
    # as `{:__aliases__, _, [:Scriba, :Test, :Source]}` AST nodes because
    # __using__ runs during macro expansion BEFORE the caller's aliases
    # are resolved. Walk the opts tree and convert alias ASTs to atoms
    # via Macro.expand/2 with the caller's env.
    opts = Macro.prewalk(opts, &resolve_alias(&1, caller_env))

    # Surface unknown keys early — typos in option names would otherwise
    # silently fall through to "default applied" behavior.
    case Enum.reject(Keyword.keys(opts), &(&1 in @valid_keys)) do
      [] -> :ok
      unknown -> raise ArgumentError, """
        Unknown option(s) #{inspect(unknown)} in use Scriba.Projection.
        Valid options: #{inspect(@valid_keys)}
        """
    end

    name = validate_name!(opts, caller_env)
    version = validate_version!(opts)
    source = validate_source_or_target!(opts, :source)
    target = validate_source_or_target!(opts, :target)
    parallelism = validate_parallelism!(opts)
    partition_by = validate_partition_by!(opts)
    handler = Keyword.get(opts, :handler, caller_env.module)

    # Pass-through opts: only included if user specified them. Pipeline's
    # own defaults apply otherwise — keeps the macro from duplicating
    # default values that live in lib/scriba/projection/pipeline.ex.
    passthrough =
      Enum.reduce([:batch_size, :batch_timeout, :retry], %{}, fn key, acc ->
        case Keyword.fetch(opts, key) do
          {:ok, value} -> Map.put(acc, key, value)
          :error -> acc
        end
      end)

    config =
      Map.merge(
        %{
          name: name,
          version: version,
          source: source,
          target: target,
          parallelism: parallelism,
          partition_by: partition_by,
          handler: handler
        },
        passthrough
      )

    {config, caller_env}
  end

  defp validate_name!(opts, caller_env) do
    case Keyword.fetch(opts, :name) do
      :error ->
        raise ArgumentError, "use Scriba.Projection requires :name"

      {:ok, name} when not is_binary(name) ->
        raise ArgumentError, ":name must be a string, got: #{inspect(name)}"

      {:ok, name} ->
        # Compile-time warning for the legacy "_v<n>" suffix pattern that
        # commanded_ecto_projections and similar libraries use. Per
        # architecture §5, name is the stable logical identity and
        # version is a separate integer.
        if Regex.match?(~r/_v\d+$/, name) do
          clean = Regex.replace(~r/_v\d+$/, name, "")

          IO.warn(
            """
            Scriba.Projection :name #{inspect(name)} matches the legacy "_v<n>" suffix pattern.
            Per SCRIBA_ARCHITECTURE.md §5, name is the stable logical identity and version is a
            separate integer option. Consider:

                use Scriba.Projection,
                  name: #{inspect(clean)},
                  version: <integer>,
                  ...
            """,
            Macro.Env.stacktrace(caller_env)
          )
        end

        name
    end
  end

  defp validate_version!(opts) do
    case Keyword.get(opts, :version, 1) do
      v when is_integer(v) and v > 0 -> v
      v -> raise ArgumentError, ":version must be a positive integer, got: #{inspect(v)}"
    end
  end

  defp validate_source_or_target!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        raise ArgumentError, "use Scriba.Projection requires :#{key}"

      {:ok, {module, module_opts}} when is_atom(module) and is_list(module_opts) ->
        {module, module_opts}

      {:ok, other} ->
        raise ArgumentError, """
        :#{key} must be a {module, opts_keyword_list} tuple, got: #{inspect(other)}
        """
    end
  end

  defp validate_parallelism!(opts) do
    case Keyword.fetch(opts, :parallelism) do
      :error ->
        raise ArgumentError, """
        use Scriba.Projection requires :parallelism. This is a deliberate decision —
        picking a default would hide a real performance trade-off. Common starting
        values: 4 for low-throughput projections, 16 for typical, schedulers_online()
        for CPU-bound handlers.
        """

      {:ok, p} when is_integer(p) and p > 0 ->
        p

      {:ok, p} ->
        raise ArgumentError, ":parallelism must be a positive integer, got: #{inspect(p)}"
    end
  end

  defp validate_partition_by!(opts) do
    case Keyword.get(opts, :partition_by, :stream_id) do
      :stream_id ->
        :stream_id

      other ->
        raise ArgumentError, """
        :partition_by must be :stream_id in v0.1; custom partitioners are post-v0.1.
        Got: #{inspect(other)}
        """
    end
  end

  ## Alias resolution

  defp resolve_alias({:__aliases__, _, _} = ast, env), do: Macro.expand(ast, env)
  defp resolve_alias(other, _env), do: other
end
