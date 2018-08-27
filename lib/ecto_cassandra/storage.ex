defmodule EctoCassandra.Storage do
  @moduledoc """
  Implement Ecto adapter storage
  """

  @behaviour Ecto.Adapter.Storage

  @replication "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"

  @spec storage_up(keyword) :: :ok | {:error, any}
  def storage_up(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)
    command = "CREATE KEYSPACE #{keyspace} WITH REPLICATION = #{@replication};"

    case Xandra.execute!(EctoCassandra.Conn, command) do
      %{effect: "CREATED"} -> :ok
      err -> {:error, err}
    end
  end

  @spec storage_down(keyword) :: :ok | {:error, any}
  def storage_down(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)

    case Xandra.execute!(EctoCassandra.Conn, "DROP KEYSPACE #{keyspace};") do
      %{effect: "DROPPED"} -> :ok
      err -> {:error, err}
    end
  end
end
