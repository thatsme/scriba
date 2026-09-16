defmodule ScribaBench.ReadModel do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "bench_rows" do
    field(:stream, :string, primary_key: true)
    field(:n, :integer, primary_key: true)
    field(:position, :integer)
  end
end
