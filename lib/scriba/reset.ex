defmodule Scriba.Reset do
  @moduledoc false

  # Clearing a projection version's progress: the rows that say where it got
  # to, and the cache entries that repeat them.
  #
  # Separate from `Scriba.` because that module is the public surface and this
  # is three deletes and a cache drop. The read model is deliberately absent —
  # Scriba does not know which tables a handler writes.

  alias Ecto.Adapters.SQL

  @type projection :: %{name: String.t(), version: pos_integer()}
  @type counts :: %{
          positions: non_neg_integer(),
          watermark: non_neg_integer(),
          dead_letters: non_neg_integer()
        }

  @doc """
  Deletes a version's cursors and watermark, optionally its dead letters, and
  drops its position-cache entries. Returns the rows removed per table.
  """
  @spec run(module(), projection(), keyword()) :: counts()
  def run(repo, %{name: name, version: version}, opts \\ []) do
    counts = %{
      positions: delete_from(repo, "scriba_positions", name, version),
      watermark: delete_from(repo, "scriba_watermarks", name, version),
      dead_letters: maybe_delete_dead_letters(repo, name, version, opts)
    }

    Scriba.Position.drop_cache(name, version)

    counts
  end

  # Kept by default: dead letters record what went wrong on the version being
  # retired, which is usually why it is being rebuilt.
  defp maybe_delete_dead_letters(repo, name, version, opts) do
    if Keyword.get(opts, :dead_letters, false) do
      delete_from(repo, "scriba_dead_letters", name, version)
    else
      0
    end
  end

  defp delete_from(repo, table, name, version) do
    %{num_rows: rows} =
      SQL.query!(
        repo,
        "DELETE FROM #{table} WHERE projection_name = $1 AND projection_version = $2",
        [name, version]
      )

    rows
  end
end
