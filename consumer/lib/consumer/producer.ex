defmodule Producer do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def check_table(name \\ "bridge_demo_users") do
    GenServer.call(__MODULE__, {:check_table, name})
  end

  def drop_table(name \\ "bridge_demo_users") do
    GenServer.call(__MODULE__, {:drop_table, name})
  end

  def create_new_table(name \\ "bridge_demo_users") do
    GenServer.call(__MODULE__, {:create_new_table, name})
  end

  def create_new_table2(name \\ "bridge_demo_orders2") do
    GenServer.call(__MODULE__, {:create_new_table, name})
  end

  def run_test(nb, name \\ "bridge_demo_users") do
    GenServer.call(__MODULE__, {:run_test, nb, name})
  end

  @impl GenServer
  def init(opts) do
    {:ok, pid} = Postgrex.start_link(opts)
    {:ok, pid, {:continue, {:create_table, "bridge_demo_users"}}}
  end

  @impl GenServer
  def handle_continue({:create_table, name}, state) do
    :ok = create_table(name, state)
    {:noreply, state}
  end

  def handle_call({:create_new_table, name}, _, state) do
    :ok = create_table(name, state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:run_test, nb, name}, _, state) do
    Logger.info("Running PG statements...")

    for i <- 1..nb do
      user_name = "User #{i}"

      %Postgrex.Result{} =
        Postgrex.query!(state, """
        INSERT INTO #{name} (name, email)
        VALUES ('#{user_name}', 'user#{i}@example.com');
        """)

      cond do
        rem(i, 5) == 0 ->
          user_name = "User #{i}"

          %Postgrex.Result{} =
            Postgrex.query!(state, "DELETE FROM #{name} WHERE name = $1", [user_name])

        rem(i, 2) == 0 ->
          %Postgrex.Result{} =
            Postgrex.query!(state, """
            UPDATE #{name} SET name = '#{user_name}', email = 'user#{i}@example.com'
            WHERE id = #{i};
            """)

        true ->
          :ok
      end
    end

    Logger.info("PG job done")

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:check_table, name}, _, state) do
    try do
      query =
        Postgrex.prepare!(state, "", "SELECT * FROM #{name};", [])

      len =
        Postgrex.execute!(state, query, [])
        |> Map.get(:num_rows)

      {:reply, len, state}
    rescue
      e in Postgrex.Error ->
        {:reply, e.postgres.message, state}
    end
  end

  @impl GenServer
  def handle_call({:drop_table, name}, _, state) do
    Postgrex.query!(state, "DROP TABLE IF EXISTS #{name};")
    {:reply, :ok, state}
  end

  defp create_table(name, pid) do
    %Postgrex.Result{} =
      Postgrex.query!(pid, """
      CREATE TABLE IF NOT EXISTS #{name} (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
      """)

    :ok
  end
end
