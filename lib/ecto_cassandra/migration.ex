defmodule EctoCassandra.Migration do
  @moduledoc """
  Implement Ecto migrations
  """

  alias Ecto.Migration.Table
  alias EctoCassandra.{Conn, Query}
  alias Xandra.SchemaChange

  @spec execute_ddl(
          repo :: Ecto.Repo.t(),
          Ecto.Adapters.Migration.command(),
          options :: Keyword.t()
        ) :: :ok | no_return
  def execute_ddl(_repo, {command, %Table{name: table_name}, commands}, _opts)
      when command in ~w(create_if_not_exists create)a do
    cql = Query.new([{command, table_name}] ++ commands)
    with %SchemaChange{effect: "CREATED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {:drop, %Table{name: table_name}}, _opts) do
    cql = Query.new(drop: table_name)
    with %SchemaChange{effect: "DROPPED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, _command, _opts) do
    raise ArgumentError, "Not acceptable arguments"
  end
end
