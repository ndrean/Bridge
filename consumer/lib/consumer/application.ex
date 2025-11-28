defmodule Consumer.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      {Producer,
       [
         hostname: System.get_env("PG_HOST") || "localhost",
         port: String.to_integer(System.get_env("PG_PORT") || "5432"),
         username: System.get_env("PG_USER") || "postgres",
         password: System.get_env("PG_PASSWORD") || "postgres",
         database: System.get_env("PG_DB") || "postgres",
         name: PgEx
       ]},
      {Gnat.ConnectionSupervisor, gnat_supervisor_settings()},
      # JetStream pull consumer for CDC events
      {Consumer.CdcConsumer, consumer_settings()}
    ]

    opts = [strategy: :one_for_one, name: Consumer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp consumer_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: System.get_env("NATS_CONSUMER_NAME") || "elixir_cdc_consumer",
      stream_name: System.get_env("NATS_STREAM_NAME") || "CDC_BRIDGE",
      ack_policy: :explicit,
      # 30 seconds in nanoseconds
      ack_wait: 30_000_000_000,
      max_deliver: 3,
      filter_subject: System.get_env("NATS_FILTER_SUBJECT") || "cdc_rt.>"
    }
  end

  defp gnat_supervisor_settings do
    %{
      name: :gnat,
      backoff_period: 4_000,
      connection_settings: [
        %{host: "localhost", port: 4222}
      ]
    }
  end
end
