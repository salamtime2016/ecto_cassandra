require IEx

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
    :in => " IN ",
    :and => "AND"
  }

  @operators Map.keys(@operators_map)

  @spec new(any) :: String.t() | no_return
  def new([{command, table_name} | commands])
      when command in ~w(create create_if_not_exists)a do
    not_exists = if command == :create_if_not_exists, do: "IF NOT EXISTS", else: ""
    {partition_keys, clustering_columns, options} = commands |> compose_keys |> format_keys

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
    "INSERT INTO #{table} (#{keys}) VALUES (#{values})#{parse_opts(opts)}"
  end

  def new(update_all: %{from: {table, _}, updates: updates, wheres: wheres}) do
    set = Enum.map_join(updates, ", ", &parse_update(&1.expr))
    new(update: {table, "SET #{set}", wheres, []})
  end

  def new(update: {table, params, filter, opts}) when is_list(params) do
    set = params |> Keyword.keys() |> Enum.map_join(", ", fn k -> "#{k} = ?" end)
    new(update: {table, "SET #{set}", filter, opts})
  end

  def new(update: {table, set, filter, opts}) do
    "UPDATE #{table} #{set} WHERE #{where(filter)}#{parse_opts(opts)}"
  end

  def new(delete_all: %Q{from: {table, _}, wheres: wheres}) do
    delete({table, wheres, []})
  end

  def new(_arg) do
    ""
  end

  @spec delete({any, list, keyword}) :: String.t()
  def delete({table, [], _opts}) do
    "TRUNCATE #{table}"
  end

  def delete({table, filters, opts}) do
    "DELETE FROM #{table} WHERE #{where(filters)}#{parse_opts(opts)}"
  end

  @spec all(Ecto.Query.t(), keyword) :: String.t()
  def all(%Q{from: {table, _}, wheres: []}, _opts) do
    "SELECT * FROM #{table}"
  end

  def all(%Q{from: {table, _}, wheres: wheres}, _opts) do
    "SELECT * FROM #{table} WHERE #{where(wheres)}"
  end

  @spec parse_opts(keyword) :: String.t()
  def parse_opts(if: opts) do
    parse_opts(opts)
  end

  def parse_opts(opts) when is_list(opts) do
    cond do
      not Keyword.has_key?(opts, :exists) -> ""
      Keyword.get(opts, :exists, true) -> " IF EXISTS"
      true -> "IF NOT EXISTS"
    end
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

  defp where(%BooleanExpr{expr: {:fragment, _, parts}}) do
    Enum.map_join(parts, " AND ", fn
      {:raw, str} -> " #{str}"
      {:expr, expr} -> parse_expr(expr)
    end)
  end

  defp where(%BooleanExpr{expr: _}) do
    raise ArgumentError, "This operator is not supported in Cassandra"
  end

  defp where([]) do
    ""
  end

  defp parse_expr({{:., [], [{:&, _, _}, key]}, _, _}) do
    key
  end

  defp parse_expr({:^, [], [_]}) do
    "?"
  end

  defp parse_expr({:^, [], [_, count]}) do
    marks =
      1..count
      |> Enum.map(fn _ -> "?" end)
      |> Enum.intersperse(", ")

    ["(", marks, ")"]
  end

  defp parse_expr({arg, [], [left, right]}) do
    "#{parse_expr(left)} #{@operators_map[arg]} #{parse_expr(right)}"
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

  defp format_keys({partition_keys, clustering_columns}) do
    partition_keys_formatted =
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

    clustering_columns_formatted =
      case length(clustering_columns) > 0 do
        true -> ", " <> Enum.map_join(clustering_columns, ", ", &elem(&1, 0))
        false -> ""
      end

    {partition_keys_formatted, clustering_columns_formatted, options}
  end

  defp parse_update([{op, expressions}]) when op in ~w(set inc push pull)a do
    for {k, v} <- expressions, do: parse_update(op, k, parse_expr(v))
  end

  defp parse_update(:set, key, value), do: "#{key} = #{value}"
  defp parse_update(:inc, key, value), do: "#{key} = #{key} + #{value}"
  defp parse_update(:push, key, value), do: "#{key} =#{key} + [#{value}]"
  defp parse_update(:pull, key, value), do: "#{key} =#{key} - [#{value}]"
end
