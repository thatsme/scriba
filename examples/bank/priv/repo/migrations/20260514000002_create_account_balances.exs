defmodule Bank.Repo.Migrations.CreateAccountBalances do
  use Ecto.Migration

  def change do
    create table(:account_balances, primary_key: false) do
      add(:account_id, :uuid, primary_key: true)
      add(:balance_cents, :bigint, null: false, default: 0)
      add(:last_event_position, :bigint, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
    end
  end
end
