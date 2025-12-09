defmodule Consumer.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      Producer.Repo,
      {Task.Supervisor, name: MyTaskSupervisor},
      {Gnat.ConnectionSupervisor, gnat_supervisor_settings()},
      {PgProducer, args()},
      # JetStream pull consumer for CDC events
      {Consumer.Init, consumer_init_settings()},
      {Consumer.Cdc, consumer_cdc_settings()}
    ]

    opts = [strategy: :one_for_one, name: Consumer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp get_tables do
    case System.get_env("TABLES") do
      nil -> ["users", "test_types"]
      tables_str -> String.split(tables_str, ",") |> Enum.map(&String.trim/1)
    end
  end

  defp args do
    [
      hostname: System.get_env("POSTGRES_HOST") || "localhost",
      port: String.to_integer(System.get_env("POSTGRES_PORT") || "5432"),
      username: System.get_env("PG_USER") || "postgres",
      password: System.get_env("PG_PASSWORD") || "postgres",
      database: System.get_env("POSTGRES_DB") || "postgres",
      name: PgEx,
      tables: get_tables() || ["users", "orders"]
    ]
  end

  defp consumer_cdc_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_cdc_consumer",
      stream_name: "CDC",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      max_batch: 100,
      deliver_policy: :all,
      filter_subject: "cdc.>"
    }
  end

  defp consumer_init_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_init_consumer",
      stream_name: "INIT",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      filter_subject: "init.>",
      deliver_policy: :all,
      max_batch: 100
    }
  end

  defp gnat_supervisor_settings do
    %{
      name: :gnat,
      backoff_period: 4_000,
      connection_settings: [
        %{
          host: System.get_env("NATS_HOST") || "127.0.0.1",
          port: String.to_integer(System.get_env("NATS_PORT") || "4222"),
          username: System.get_env("NATS_USER"),
          password: System.get_env("NATS_PASSWORD")
          #   tls: %{
          #   required: true,
          #   verify: true,
          #   cacertfile: "/path/to/ca.pem"
          # }
        }
      ]
    }
  end
end
