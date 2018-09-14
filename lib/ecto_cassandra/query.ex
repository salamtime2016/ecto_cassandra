defmodule EctoCassandra.Query do
  @moduledoc """
  Compose CQL query from Ecto.Query
  """

  require Logger

  alias Ecto.Query, as: Q
  alias Ecto.Query.{BooleanExpr}
  alias EctoCassandra.Types

  @spec new(any) :: String.t() | no_return
  def new([{:create_if_not_exists, table_name} | commands]) do
    "CREATE TABLE IF NOT EXISTS #{table_name} (#{new(commands)})"
  end

  def new([{:create, table_name} | commands]) do
    "CREATE TABLE #{table_name} (#{new(commands)})"
  end

  def new([{:alter, table_name} | commands]) do
    "ALTER TABLE #{table_name} #{alter_commands(commands)}"
  end

  def new([{:add, column, type, options} | commands]) do
    primary_key? = if Keyword.get(options, :primary_key), do: "PRIMARY KEY", else: ""
    "#{column} #{Types.to_db(type)} #{primary_key?}, " <> new(commands)
  end

  def new(rename: [table, from, to]) do
    "ALTER TABLE #{table} RENAME #{from} TO #{to}"
  end

  def new(drop: table_name) do
    "DROP TABLE #{table_name}"
  end

  def new(create_index: {table, columns, index_name}) do
    indexed_columns = Enum.map_join(columns, ",", &to_string/1)
    "CREATE INDEX #{index_name} ON #{table} (#{indexed_columns})"
  end

  def new(drop_index: index_name) do
    "DROP INDEX #{index_name}"
  end

  def new(insert: {table, keys, values, opts}) do
    opts =
      case Keyword.get(opts, :if_not_exists, false) do
        true -> "IF NOT EXISTS"
        _ -> ""
      end

    "INSERT INTO #{table} (#{keys}) VALUES (#{values}) #{opts}"
  end

  def new(update: {table, params, filter}) do
    set = params |> Keyword.keys() |> Enum.map_join(", ", fn k -> "#{k} = ?" end)
    "UPDATE #{table} SET #{set} WHERE #{where(filter)}"
  end

  def new(delete_all: %Q{from: {table, _}, wheres: wheres}) do
    "DELETE FROM #{table} WHERE #{where(wheres)}"
  end

  def new(delete: {table, filters}) do
    "DELETE FROM #{table} WHERE #{where(filters)}"
  end

  def new(_arg) do
    ""
  end

  @spec all(Ecto.Query.t(), keyword) :: String.t()
  def all(%Q{from: {table, _}, wheres: []}, _opts) do
    "SELECT * FROM #{table}"
  end

  def all(%Q{from: {table, _}, wheres: wheres}, opts) do
    allow_filtering =
      case Keyword.get(opts, :allow_filtering, false) do
        true ->
          Logger.warn(fn -> "Prefer to use primary keys instead of ALLOW FILTERING" end)
          " ALLOW FILTERING"

        false ->
          ""
      end

    "SELECT * FROM #{table} WHERE #{where(wheres)} #{allow_filtering}"
  end

  defp alter_commands([{:add, column, type, options} | commands]) do
    primary_key? = if Keyword.get(options, :primary_key), do: "PRIMARY KEY", else: ""
    columns = "#{column} #{Types.to_db(type)} #{primary_key?}"

    "ADD #{columns}" <> alter_commands(commands)
  end

  defp alter_commands([{:remove, column} | commands]) do
    "DROP #{column}" <> alter_commands(commands)
  end

  defp alter_commands([]) do
    ""
  end

  defp where([expr | wheres]) do
    where(expr) <> where(wheres)
  end

  defp where({key, val}) do
    "#{key} = #{val}"
  end

  defp where(%BooleanExpr{expr: {op, [], [left, _right]}}) do
    {{_arg, [], [{:&, [], [0]}, field]}, [], []} = left
    "#{field} #{op_to_cql(op)} ?"
  end

  defp where([]) do
    ""
  end

  # Converts Ecto operators to CQL operators
  defp op_to_cql(:==), do: "="
  defp op_to_cql(op), do: to_string(op)
end
