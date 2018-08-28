defmodule EctoCassandra.Migration do
  @moduledoc """
  Implement Ecto migrations
  """

  alias Ecto.Migration.Table
  alias EctoCassandra.Conn
  alias Xandra.SchemaChange

  @spec execute_ddl(
          repo :: Ecto.Repo.t(),
          Ecto.Adapters.Migration.command(),
          options :: Keyword.t()
        ) :: :ok | no_return
  def execute_ddl(repo, {command, %Table{name: table_name}, commands}, _opts)
      when command in ~w(create_if_not_exists create)a do
    cql = to_cql([{command, table_name}] ++ commands)

    with {:ok, %SchemaChange{effect: "CREATED"}} <- Xandra.execute(Conn, cql), do: :ok
  end

  def execute_ddl(_repo, command, _opts) do
    raise ArgumentError, "Not acceptable arguments"
  end

  defp to_cql([{:create_if_not_exists, table_name} | commands]) do
    "CREATE TABLE IF NOT EXISTS #{table_name} (#{to_cql(commands)});"
  end

  defp to_cql([{:create, table_name} | commands]) do
    "CREATE TABLE #{table_name} (#{to_cql(commands)});"
  end

  defp to_cql([{:add, column, type, options} | commands]) do
    primary_key? = if Keyword.get(options, :primary_key), do: "PRIMARY KEY", else: ""
    "#{column} #{to_db(type)} #{primary_key?}" <> to_cql(commands)
  end

  defp to_cql(arg) do
    ""
  end

  defp to_db(time) when time in ~w(datetime naive_datetime utc_datetime)a, do: "timestamp"
  defp to_db(field) when field in ~w(id integer), do: "int"
  defp to_db(:binary_id), do: "uuid"
  defp to_db(:binary), do: "blob"
  defp to_db(:string), do: "text"
  defp to_db(:map), do: to_db({:map, :binary})
  defp to_db({:map, {t1, t2}}), do: "map<#{to_db(t1)}, #{to_db(t2)}>"
  defp to_db({:map, t1, t2}), do: "map<#{to_db(t1)}, #{to_db(t2)}>"
  defp to_db({:map, t}), do: to_db({:map, {:varchar, t}})
  defp to_db({:array, t}), do: "list<#{to_db(t)}>"
  defp to_db({:list, t}), do: "list<#{to_db(t)}>"
  defp to_db({:set, t}), do: "set<#{to_db(t)}>"
  defp to_db({:tuple, type}) when is_atom(type), do: to_db({:tuple, {type}})
  defp to_db({:frozen, type}), do: "frozen<#{to_db(type)}>"

  defp to_db(:serial) do
    raise(ArgumentError, "Cassandra does not support :serial type")
  end

  defp to_db({:tuple, types}) when is_tuple(types) do
    types_defintion =
      types
      |> Tuple.to_list()
      |> Enum.map_join(", ", &to_db/1)

    "tuple<#{types_defintion}>"
  end

  defp to_db(any), do: to_string(any)
end
