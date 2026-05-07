defmodule Scriba do
  @moduledoc """
  Public API surface for Scriba projections.

  In  only `info/2` is implemented; the lifecycle calls
  (`start_projection/1`, `pause/1`, `resume/1`, `stop/1`, `list/0`) come
  in .
  """

  alias Scriba.Info

  @stream_position_truncation_limit 1000

  @doc """
  Returns a snapshot of the projection's runtime state. See `Scriba.Info` for
  the shape.

  Returns `{:error, :not_found}` if no Coordinator is registered for the
  given `(name, version)`.

  When the projection has more than #{@stream_position_truncation_limit}
  distinct streams, `:stream_positions` is `:truncated` rather than a giant
  map; `:safe_position` is always populated regardless.
  """
  @spec info(String.t(), pos_integer()) :: {:ok, Info.t()} | {:error, :not_found}
  def info(name, version \\ 1) do
    case Scriba.Projection.Coordinator.get_status(name, version) do
      {:ok, status} ->
        {:ok, build_info(name, version, status)}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  defp build_info(name, version, status) do
    streams = Scriba.Position.stream_positions(name, version)
    safe = Scriba.Position.safe_position(name, version)

    stream_positions =
      if map_size(streams) > @stream_position_truncation_limit do
        :truncated
      else
        streams
      end

    %Info{
      name: name,
      version: version,
      status: Map.get(status, :state),
      source: Map.get(status, :source),
      target: Map.get(status, :target),
      safe_position: safe,
      stream_positions: stream_positions
    }
  end
end
