defmodule User do
  use Ecto.Schema

  schema "users" do
    field(:name, :string)
    field(:email, :string)

    timestamps()
  end
end

defmodule Order do
  use Ecto.Schema

  schema "orders" do
    field(:name, :string)
    field(:email, :string)

    timestamps()
  end
end
