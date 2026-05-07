defmodule Scriba.Test.ReadModel do
  @moduledoc """
  Minimal read-model schema for property_db tests ().

  Three columns: `event_id` (string, primary key), `stream_id`, `position`.
  Migration in `test/support/migrations/create_test_read_models.ex`,
  invoked from `test_helper.exs` after the Scriba tables migration.

  Property tests insert one row per non-skipped event. The `event_id` PK
  catches double-applies; the `(stream_id, position)` pair lets PD2 assert
  cursor consistency against `scriba_positions`.
  """

  use Ecto.Schema

  @primary_key {:event_id, :string, autogenerate: false}
  schema "test_read_models" do
    field :stream_id, :string
    field :position, :integer
  end
end
