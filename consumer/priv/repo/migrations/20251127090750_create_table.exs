defmodule Producer.Repo.Migrations.CreateTable do
  use Ecto.Migration

  def change do
    create table("bridge_demo_users") do
      add(:name, :text, null: false)
      add(:email, :text)
      timestamps()
    end
  end

  def down do
    drop(table("bridge_demo_users"))
  end
end
