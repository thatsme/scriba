defmodule ScribaBench.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :scriba_bench, adapter: Ecto.Adapters.Postgres
end
