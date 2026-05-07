defmodule Scriba.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    Scriba.Supervisor.start_link([])
  end
end
