defmodule Scriba.Registry do
  @moduledoc false

  def child_spec(_opts) do
    Supervisor.child_spec(
      {Registry, keys: :unique, name: __MODULE__},
      id: __MODULE__
    )
  end
end
