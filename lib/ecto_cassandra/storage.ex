defmodule EctoCassandra.Storage do
  @moduledoc """
  Implement Ecto adapter storage
  """

  @behaviour Ecto.Adapter.Storage
  alias EctoCassandra.Connection

  @replication "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"

  @spec storage_up(keyword) :: :ok | {:error, any}
  def storage_up(options) when is_list(options) do
    with [keyspace: keyspace, conn: conn] <- resolve_options(options),
         command <- "CREATE KEYSPACE #{keyspace} WITH REPLICATION = #{@replication};",
         %{effect: "CREATED"} <- Xandra.execute!(conn, command) do
      :ok
    else
      err -> {:error, err}
    end
  end

  @spec storage_down(keyword) :: :ok | {:error, any}
  def storage_down(options) when is_list(options) do
    with [keyspace: keyspace, conn: conn] <- resolve_options(options),
         %{effect: "DROPPED"} <- Xandra.execute!(conn, "DROP KEYSPACE #{keyspace};") do
      :ok
    end
  end

  defp resolve_options(options) do
    {:ok, conn} = Connection.init(options)
    [keyspace: Keyword.fetch!(options, :keyspace), conn: conn]
  end
end
