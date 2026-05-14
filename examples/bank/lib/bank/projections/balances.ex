defmodule Bank.Projections.Balances do
  @moduledoc """
  Maintains the `account_balances` read model — one row per account,
  updated by each domain event.

  This is the showcase file for `use Scriba.Projection`. Five lines
  of declaration plus four `handle/2` clauses.

  Deposit/Withdraw use `Ecto.Multi.update_all` with `:inc` so the
  database computes `balance_cents = balance_cents + amount` atomically
  in SQL — no read-modify-write race. Architecture §4.2's
  `{:update, _, _, [set: ...]}` shape doesn't expose `:inc`; the
  `{:multi, _}` shape lets us reach the full Ecto.Multi API for
  cases like this.
  """

  use Scriba.Projection,
    name: "account_balances",
    source: {Scriba.Source.Commanded, application: Bank.CommandedApp},
    target: {Scriba.Target.Ecto, repo: Bank.Repo},
    parallelism: 4

  import Ecto.Query, only: [from: 2]

  alias Bank.Events.{AccountOpened, Deposited, Withdrew}
  alias Bank.ReadModels.AccountBalance

  def handle(%AccountOpened{account_id: id}, %{position: pos}) do
    {:insert,
     %AccountBalance{
       account_id: id,
       balance_cents: 0,
       last_event_position: pos,
       updated_at: DateTime.utc_now()
     }}
  end

  def handle(%Deposited{account_id: id, amount_cents: amt}, %{position: pos}) do
    {:multi, balance_update_multi(id, amt, pos)}
  end

  def handle(%Withdrew{account_id: id, amount_cents: amt}, %{position: pos}) do
    {:multi, balance_update_multi(id, -amt, pos)}
  end

  # Anything else (e.g. a future event type added before the projection
  # is upgraded) is acknowledged without side effect. Required because
  # missing-clause errors during a deploy would dead-letter every
  # unfamiliar event.
  def handle(_event, _meta), do: :skip

  defp balance_update_multi(account_id, delta_cents, pos) do
    query = from(b in AccountBalance, where: b.account_id == ^account_id)

    # Key includes `pos` so multiple events for the same account in one
    # batch don't collide — Scriba.Target.Ecto merges all per-event
    # Multis into one, and Ecto.Multi.merge/2 requires globally unique
    # step keys.
    Ecto.Multi.update_all(
      Ecto.Multi.new(),
      {:balance, account_id, pos},
      query,
      inc: [balance_cents: delta_cents],
      set: [last_event_position: pos, updated_at: DateTime.utc_now()]
    )
  end
end
