defmodule Bank.CommandedApp do
  @moduledoc """
  Commanded application for the bank demo. Wires the router into a
  Commanded application; event-store adapter is configured in
  `config/config.exs` (InMemory by default).

  This is what `Scriba.Source.Commanded` subscribes to — passing
  `{Scriba.Source.Commanded, application: Bank.CommandedApp}` as the
  projection's `:source` connects Scriba to this Commanded app's
  event store.
  """

  use Commanded.Application, otp_app: :bank

  router(Bank.Router)
end
