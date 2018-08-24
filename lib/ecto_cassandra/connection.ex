defmodule EctoCassandra.Connection do
  @moduledoc """
  Connect to Cassandra
  """

  @default_host "127.0.0.1"
  @default_port 9042

  @spec init(keyword) :: {:ok, pid}
  def init(opts) when is_list(opts) do
    [host, port] = [
      Keyword.get(opts, :host, @default_host),
      Keyword.get(opts, :port, @default_port)
    ]

    with nil <- Process.whereis(EctoCassandra.Conn),
         {:ok, conn} <- Xandra.start_link(nodes: ["#{host}:#{port}"]),
         true <- Process.register(conn, EctoCassandra.Conn) do
      {:ok, conn}
    end
  end
end
