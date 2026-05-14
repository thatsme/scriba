defmodule Bank.ReadModels.AccountBalance do
  @moduledoc """
  Read model for one bank account's current balance.

  The `last_event_position` column is deliberately redundant with
  Scriba's own `scriba_positions` table — included here to demonstrate
  how the `:position` field of the handler's `meta` map flows into
  user-controlled read-model state. Most projections won't need it;
  some find it useful for "this row was last touched by event N"
  debugging.

  Primary key is the account UUID. `Scriba.Target.Ecto` upserts via
  the read model's primary key when handlers return `{:insert, _}`
  for the first event and `{:update, _, _, _}` for subsequent ones.
  """

  use Ecto.Schema

  @primary_key {:account_id, Ecto.UUID, autogenerate: false}
  schema "account_balances" do
    field(:balance_cents, :integer)
    field(:last_event_position, :integer)

    timestamps(type: :utc_datetime_usec, inserted_at: false)
  end
end
