defmodule Bank.Events do
  @moduledoc """
  Domain events for the bank demo. Three events, deliberately minimal:

    * `AccountOpened` — first event for a new account.
    * `Deposited` — adds to balance.
    * `Withdrew` — subtracts from balance.

  All amounts are in cents (integer). `account_id` is a UUID string.
  Events serialize via the structs themselves — Commanded's InMemory
  adapter passes them through without JSON-encoding (see
  `config/config.exs`).
  """

  defmodule AccountOpened do
    @moduledoc false
    defstruct [:account_id, :opened_at]
  end

  defmodule Deposited do
    @moduledoc false
    defstruct [:account_id, :amount_cents]
  end

  defmodule Withdrew do
    @moduledoc false
    defstruct [:account_id, :amount_cents]
  end
end
