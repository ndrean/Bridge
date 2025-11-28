defmodule Producer.Repo do
  use Ecto.Repo,
    otp_app: :consumer,
    adapter: Ecto.Adapters.Postgres
end
