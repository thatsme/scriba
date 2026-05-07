defmodule Scriba.Test.ReadModel do
  @moduledoc false

  use Ecto.Schema

  schema "test_read_models" do
    field :name, :string
    field :status, :string
  end
end
