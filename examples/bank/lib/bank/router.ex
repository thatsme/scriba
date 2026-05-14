defmodule Bank.Router do
  @moduledoc """
  Commanded router — maps commands to the `Bank.Account` aggregate.

  `identity: :account_id` tells Commanded to route every command to
  the aggregate instance keyed by the command's `:account_id` field.
  Same UUID → same aggregate process → events from that account are
  produced in order. (Scriba's per-stream ordering invariant then
  preserves that order through to the read model.)
  """

  use Commanded.Commands.Router

  alias Bank.Account
  alias Bank.Commands.{OpenAccount, Deposit, Withdraw}

  dispatch([OpenAccount, Deposit, Withdraw],
    to: Account,
    identity: :account_id
  )
end
