defmodule EctoCassandra.Storage do
  @moduledoc """
  Implement Ecto adapter storage
  """

  @behaviour Ecto.Adapter.Storage
  @default_replication_opts [class: "SimpleStrategy", replication_factor: 3]

  @spec storage_up(Keyword.t()) :: :ok | {:error, any}
  def storage_up(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)
    command = "CREATE KEYSPACE #{keyspace} WITH REPLICATION = #{configure_replication(options)};"

    with {:ok, conn} <- Xandra.start_link(options),
         {:ok, %{effect: "CREATED"}} <- Xandra.execute(conn, command) do
      :ok
    end
  end

  @spec storage_down(Keyword.t()) :: :ok | {:error, any}
  def storage_down(options) when is_list(options) do
    keyspace = Keyword.fetch!(options, :keyspace)

    with {:ok, conn} <- Xandra.start_link(options),
         {:ok, %{effect: "DROPPED"}} <- Xandra.execute(conn, "DROP KEYSPACE #{keyspace};") do
      :ok
    end
  end

  defp configure_replication(options) do
    [class: class, replication_factor: replication_factor] =
      Keyword.merge(@default_replication_opts, Keyword.get(options, :replication))

    "{ 'class' : '#{class}', 'replication_factor' : #{replication_factor} }"
  end
end
