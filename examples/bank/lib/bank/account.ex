defmodule Bank.Account do
  @moduledoc """
  Aggregate for a bank account. Tracks balance in memory and validates
  the one rule the demo cares about: "you can't deposit/withdraw before
  the account is opened."

  **No overdraft check.** The demo allows negative balances by design —
  this keeps the example about Scriba projection mechanics, not
  aggregate-design lessons that belong in Commanded's own
  documentation. The read model will faithfully show negative balances
  when they happen.
  """

  alias Bank.Commands.{OpenAccount, Deposit, Withdraw}
  alias Bank.Events.{AccountOpened, Deposited, Withdrew}

  # Default :opened? to false (not nil) — Commanded constructs the
  # struct via %Bank.Account{} when the aggregate doesn't exist yet,
  # so the defstruct default IS the initial state. Falsy-not-nil
  # matters because execute clauses pattern-match on :opened?.
  defstruct account_id: nil, balance_cents: 0, opened?: false

  ## Command execution

  def execute(%__MODULE__{opened?: false}, %OpenAccount{account_id: id}) do
    %AccountOpened{account_id: id, opened_at: DateTime.utc_now()}
  end

  def execute(%__MODULE__{opened?: true}, %OpenAccount{}),
    do: {:error, :account_already_open}

  def execute(%__MODULE__{opened?: false}, %Deposit{}),
    do: {:error, :account_not_open}

  def execute(%__MODULE__{opened?: true}, %Deposit{} = cmd) do
    %Deposited{account_id: cmd.account_id, amount_cents: cmd.amount_cents}
  end

  def execute(%__MODULE__{opened?: false}, %Withdraw{}),
    do: {:error, :account_not_open}

  def execute(%__MODULE__{opened?: true}, %Withdraw{} = cmd) do
    %Withdrew{account_id: cmd.account_id, amount_cents: cmd.amount_cents}
  end

  ## State transitions

  def apply(%__MODULE__{} = state, %AccountOpened{account_id: id}) do
    %{state | account_id: id, balance_cents: 0, opened?: true}
  end

  def apply(%__MODULE__{} = state, %Deposited{amount_cents: amt}) do
    %{state | balance_cents: state.balance_cents + amt}
  end

  def apply(%__MODULE__{} = state, %Withdrew{amount_cents: amt}) do
    %{state | balance_cents: state.balance_cents - amt}
  end
end
