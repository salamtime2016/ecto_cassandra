defmodule EctoCassandra.Migration do
  @moduledoc """
  Implement Ecto migrations
  """

  alias Ecto.Migration.{Index, Table}
  alias EctoCassandra.{Conn, Query}
  alias Xandra.SchemaChange

  @spec execute_ddl(
          repo :: Ecto.Repo.t(),
          Ecto.Adapter.Migration.command(),
          options :: Keyword.t()
        ) :: :ok | no_return
  def execute_ddl(_repo, {command, %Table{name: table_name}, commands}, _opts)
      when command in ~w(create_if_not_exists create)a do
    cql = Query.new([{command, table_name}] ++ commands)
    with %SchemaChange{effect: "CREATED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {command, %Index{columns: columns, name: name, table: table}}, _opts)
      when command in ~w(create_if_not_exists create)a do
    cql = Query.new(create_index: {table, columns, name})
    with %SchemaChange{effect: "CREATED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {:drop, %Index{name: name}}, _opts) do
    cql = Query.new(drop_index: name)
    with %SchemaChange{effect: "DROPPED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {:drop, %Table{name: table_name}}, _opts) do
    cql = Query.new(drop: table_name)
    with %SchemaChange{effect: "DROPPED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {:alter, %Table{name: table_name}, commands}, _opts) do
    cql = Query.new([{:alter, table_name}] ++ commands)
    with %SchemaChange{effect: "CHANGED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, {:rename, %Table{name: table_name}, from, to}, _opts) do
    cql = Query.new(rename: [table_name, from, to])
    with %SchemaChange{effect: "CHANGED"} <- Xandra.execute!(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, _command, _opts) do
    raise ArgumentError, "Not acceptable arguments"
  end
end
