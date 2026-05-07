defmodule Scriba.Partitioner do
  @moduledoc """
  Routes events to workers by hashing `stream_id`.

  Same `stream_id` always hashes to the same partition for a fixed number of
  partitions. `:erlang.phash2/2` distributes uniformly across the result
  range, which is what the per-stream-ordering guarantee in §3.2 requires.

  This is plain modular hashing, **not** ring-based consistent hashing. The
  number of partitions is fixed at projection start (`parallelism: N`); when
  it changes, the projection is restarted from scratch (or, more typically,
  a new `version` is started side-by-side and cut over). Ring-style minimal
  remapping has no use case here.
  """

  @doc """
  Returns the partition index in the range `0..partitions - 1` for `stream_id`.

  Raises if `partitions` is not a positive integer.
  """
  @spec partition(stream_id :: term(), partitions :: pos_integer()) :: non_neg_integer()
  def partition(stream_id, partitions) when is_integer(partitions) and partitions > 0 do
    :erlang.phash2(stream_id, partitions)
  end
end
