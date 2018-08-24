defmodule EctoCassandra.Planner do
  @moduledoc """
  Ecto Cassandra core planner
  """

  require Logger
  @behaviour Ecto.Adapter

  @doc false
  defmacro __before_compile__(_env), do: :ok

  def ensure_all_started(_repo, type) do
    {:ok, []}
  end

  def child_spec(_repo, _opts),
    do: Supervisor.Spec.supervisor(Supervisor, [[], [strategy: :one_for_one]])

  @doc """
  Automatically generate next ID for binary keys, leave sequence keys empty for generation on insert.
  """
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.autogenerate()

  def autogenerate(:id) do
    raise(
      ArgumentError,
      "Cassandra adapter does not support autogenerated :id field type in schema."
    )
  end

  def prepare(operation, query), do: raise_not_implemented_error()

  def execute(repo, query_meta, query_cache, sources, preprocess, opts),
    do: raise_not_implemented_error()

  def insert(repo, query_meta, sources, on_conflict, returning, opts),
    do: raise_not_implemented_error()

  def insert_all(repo, query_meta, header, rows, on_conflict, returning, opts),
    do: raise_not_implemented_error()

  def update(repo, query_meta, params, filter, autogen, opts), do: raise_not_implemented_error()

  def delete(repo, query_meta, filter, opts), do: raise_not_implemented_error()

  def transaction(repo, opts, fun), do: raise_not_implemented_error()

  def loaders(primitive, type), do: raise_not_implemented_error()

  def dumpers(primitive, type), do: raise_not_implemented_error()

  defp raise_not_implemented_error, do: raise(ArgumentError, "Not implemented")
end
