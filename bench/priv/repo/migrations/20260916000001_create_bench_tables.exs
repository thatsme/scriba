defmodule ScribaBench.Repo.Migrations.CreateBenchTables do
  use Ecto.Migration

  def up do
    Scriba.Migrations.up()

    create table(:bench_rows, primary_key: false) do
      add(:stream, :string, size: 255, null: false, primary_key: true)
      add(:n, :integer, null: false, primary_key: true)
      add(:position, :bigint, null: false)
    end
  end

  def down do
    drop(table(:bench_rows))
    Scriba.Migrations.down()
  end
end
