defmodule EctoCassandra.Query do
  @moduledoc """
  Compose CQL query from Ecto.Query
  """

  alias Ecto.Query, as: Q
  alias EctoCassandra.Types

  @spec new(any) :: String.t() | no_return
  @spec new(atom, Q.t()) :: String.t() | no_return
  def new([{:create_if_not_exists, table_name} | commands]) do
    "CREATE TABLE IF NOT EXISTS #{table_name} (#{new(commands)});"
  end

  def new([{:create, table_name} | commands]) do
    "CREATE TABLE #{table_name} (#{new(commands)});"
  end

  def new([{:add, column, type, options} | commands]) do
    primary_key? = if Keyword.get(options, :primary_key), do: "PRIMARY KEY", else: ""
    "#{column} #{Types.to_db(type)} #{primary_key?}" <> new(commands)
  end

  def new(arg) do
    ""
  end

  def new(:all, %Q{from: {table, _}, select: select}) do
    "SELECT * FROM #{table}"
  end

  def new(_, _) do
    ""
  end
end
