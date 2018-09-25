defmodule EctoCassandra.Query do
  @moduledoc """
  Compose CQL query from Ecto.Query
  """

  require Logger

  alias Ecto.Query, as: Q
  alias Ecto.Query.{BooleanExpr}
  alias EctoCassandra.Types

  @operators_map %{
    :== => "=",
    :< => "<",
    :> => ">",
    :<= => "<=",
    :>= => ">=",
    :!= => "!=",
    :and => "AND"
  }

  @operators Map.keys(@operators_map)

  @spec new(any) :: String.t() | no_return
  def new([{command, table_name} | commands])
      when command in ~w(create create_if_not_exists)a do
    not_exists = if command == :create_if_not_exists, do: "IF NOT EXISTS", else: ""

    {partition_keys, clustering_columns} = compose_keys(commands)

    partition_keys =
      case length(partition_keys) > 1 do
        true -> "(#{Enum.join(partition_keys, ", ")})"
        false -> partition_keys |> hd |> to_string
      end

    options =
      case length(clustering_columns) > 0 do
        true ->
          order =
            Enum.map_join(clustering_columns, ",", fn {key, direction} ->
              "#{key} #{direction}"
            end)

          "WITH CLUSTERING ORDER BY (#{order})"

        false ->
          ""
      end

    clustering_columns =
      case length(clustering_columns) > 0 do
        true -> ", " <> Enum.map_join(clustering_columns, ", ", &elem(&1, 0))
        false -> ""
      end

    "CREATE TABLE #{not_exists} #{table_name} (#{compose_columns(commands)} PRIMARY KEY (#{
      partition_keys
    }#{clustering_columns})) #{options}"
  end

  def new([{:alter, table_name} | commands]) do
    "ALTER TABLE #{table_name} #{alter_commands(commands)}"
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
    "INSERT INTO #{table} (#{keys}) VALUES (#{values}) #{parse_upsert_opts(opts)}"
  end

  def new(update: {table, params, filter, opts}) do
    set = params |> Keyword.keys() |> Enum.map_join(", ", fn k -> "#{k} = ?" end)
    "UPDATE #{table} SET #{set} WHERE #{where(filter)} #{parse_upsert_opts(opts)}"
  end

  def new(delete_all: %Q{from: {table, _}, wheres: wheres}) do
    new(delete: {table, wheres})
  end

  def new(delete: {table, []}) do
    "TRUNCATE #{table}"
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

  def all(%Q{from: {table, _}, wheres: wheres}, _opts) do
    "SELECT * FROM #{table} WHERE #{where(wheres)}"
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

  defp where(%BooleanExpr{expr: {op, [], [left, right]}}) when op in @operators do
    "#{parse_expr(left)} #{@operators_map[op]} #{parse_expr(right)}"
  end

  defp where(%BooleanExpr{expr: _}) do
    raise ArgumentError, "This operator is not supported in Cassandra"
  end

  defp where([]) do
    ""
  end

  defp parse_expr({arg, [], [left, right]}) do
    "#{parse_expr(left)} #{@operators_map[arg]} #{parse_expr(right)}"
  end

  defp parse_expr({{:., [], [{:&, _, _}, key]}, _, _}) do
    key
  end

  defp parse_expr({:^, [], [_, count]}) do
    questions =
      1..count
      |> Enum.map(fn _ -> "?" end)
      |> Enum.intersperse(", ")

    ["(", questions, ")"]
  end

  defp parse_expr({:^, [], [_]}) do
    "?"
  end

  defp compose_columns([{:add, column, type, _options} | commands]) do
    "#{column} #{Types.to_db(type)}, " <> compose_columns(commands)
  end

  defp compose_columns(_) do
    ""
  end

  defp compose_keys(commands) when is_list(commands) do
    Enum.reduce(commands, {[], []}, &compose_keys/2)
  end

  defp compose_keys({:add, field, _type, opts}, {partition_keys, clustering_columns} = acc) do
    clustering_order = Keyword.get(opts, :clustering_column)

    cond do
      Keyword.get(opts, :primary_key, false) ->
        {[field | partition_keys], clustering_columns}

      Keyword.get(opts, :partition_key, false) ->
        {[field | partition_keys], clustering_columns}

      clustering_order in ~w(asc desc)a ->
        {partition_keys, [{field, clustering_order} | clustering_columns]}

      true ->
        acc
    end
  end

  defp compose_keys(_, acc) do
    acc
  end

  defp parse_upsert_opts(opts) when is_list(opts) do
    cond do
      Keyword.get(opts, :if_not_exists, false) -> "IF NOT EXISTS"
      Keyword.get(opts, :if_exists, false) -> "IF EXISTS"
      true -> ""
    end
  end
end
