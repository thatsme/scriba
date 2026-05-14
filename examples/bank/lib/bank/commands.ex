defmodule Bank.Commands do
  @moduledoc """
  Commands dispatched against the `Bank.Account` aggregate.
  Mirrors the event shapes one-to-one in this demo — real apps would
  often have richer commands than the events they produce.
  """

  defmodule OpenAccount do
    @moduledoc false
    defstruct [:account_id]
  end

  defmodule Deposit do
    @moduledoc false
    defstruct [:account_id, :amount_cents]
  end

  defmodule Withdraw do
    @moduledoc false
    defstruct [:account_id, :amount_cents]
  end
end
