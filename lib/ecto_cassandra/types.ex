defmodule EctoCassandra.Types do
  @moduledoc """
  Ecto to Cassandra types conversions
  """

  @spec to_db(any) :: any
  def to_db(time) when time in ~w(datetime naive_datetime utc_datetime)a, do: "timestamp"
  def to_db(field) when field in ~w(id integer)a, do: "bigint"
  def to_db(:binary_id), do: "uuid"
  def to_db(:binary), do: "blob"
  def to_db(:string), do: "text"
  def to_db(:map), do: to_db({:map, :binary})
  def to_db({:map, {t1, t2}}), do: "map<#{to_db(t1)}, #{to_db(t2)}>"
  def to_db({:map, t1, t2}), do: "map<#{to_db(t1)}, #{to_db(t2)}>"
  def to_db({:map, t}), do: to_db({:map, {:varchar, t}})
  def to_db({:array, t}), do: "list<#{to_db(t)}>"
  def to_db({:list, t}), do: "list<#{to_db(t)}>"
  def to_db({:set, t}), do: "set<#{to_db(t)}>"
  def to_db({:tuple, type}) when is_atom(type), do: to_db({:tuple, {type}})
  def to_db({:frozen, type}), do: "frozen<#{to_db(type)}>"

  def to_db(:serial) do
    raise(ArgumentError, "Cassandra does not support :serial type")
  end

  def to_db({:tuple, types}) when is_tuple(types) do
    types_defintion =
      types
      |> Tuple.to_list()
      |> Enum.map_join(", ", &to_db/1)

    "tuple<#{types_defintion}>"
  end

  def to_db(any), do: to_string(any)
end
