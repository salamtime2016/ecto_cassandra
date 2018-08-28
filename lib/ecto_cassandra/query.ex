defmodule EctoCassandra.Query do
  @moduledoc """
  Compose CQL query from Ecto.Query
  """

  alias Ecto.Query, as: Q

  @spec new(atom, Q.t()) :: String.t() | no_return
  def new(:all, %Q{from: {table, _}, select: select}) do
    "SELECT * FROM #{table}"
  end

  def new(_, _) do
    raise ArgumentError, "Not acceptable arguments"
  end
end
