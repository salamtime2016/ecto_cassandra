defmodule EctoCassandra.Storage do
  @moduledoc """
  Implement Ecto adapter storage
  """

  @behaviour Ecto.Adapter.Storage

  @replication "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"

  @spec storage_up(keyword) :: :ok | {:error, any}
  def storage_up(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)
    # TODO: implement replication opts
    command = "CREATE KEYSPACE #{keyspace} WITH REPLICATION = #{@replication};"

    with {:ok, conn} <- Xandra.start_link(options),
         %{effect: "CREATED"} <- Xandra.execute!(conn, command) do
      :ok
    else
      err -> {:error, err}
    end
  end

  @spec storage_down(keyword) :: :ok | {:error, any}
  def storage_down(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)

    with {:ok, conn} <- Xandra.start_link(options),
         %{effect: "DROPPED"} <- Xandra.execute!(conn, "DROP KEYSPACE #{keyspace};") do
      :ok
    else
      err -> {:error, err}
    end
  end
end
