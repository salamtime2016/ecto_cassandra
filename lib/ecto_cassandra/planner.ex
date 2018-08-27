defmodule EctoCassandra.Planner do
  @moduledoc """
  Ecto Cassandra core planner
  """

  require Logger
  alias Ecto.Query

  @behaviour Ecto.Adapter

  @doc false
  defmacro __before_compile__(_env), do: :ok

  @spec ensure_all_started(any, type :: :application.restart_type()) ::
          {:ok, [atom]} | {:error, atom}
  def ensure_all_started(_repo, _type) do
    {:ok, []}
  end

  @spec child_spec(any, keyword) :: Supervisor.Spec.spec()
  def child_spec(_repo, opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)

    opts =
      Keyword.merge(opts,
        name: EctoCassandra.Conn,
        after_connect: &Xandra.execute(&1, "USE #{keyspace}")
      )

    Supervisor.Spec.supervisor(Xandra, [opts], restart: :permanent, id: EctoCassandra.Conn)
  end

  @doc """
  Automatically generate next ID for binary keys, leave sequence keys empty for generation on insert.
  """
  @spec autogenerate(atom) :: String.t() | no_return
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.autogenerate()

  def autogenerate(:id) do
    raise(
      ArgumentError,
      "Cassandra adapter does not support autogenerated :id field type in schema."
    )
  end

  @spec prepare(atom :: :all | :update_all | :delete_all, Query.t()) ::
          {:cache, term} | {:nocache, term} | no_return
  def prepare(operation, query) do
    with {:ok, prepared} <-
           Xandra.prepare(EctoCassandra.Conn, EctoCassandra.Query.new(operation, query)),
         do: {:cache, prepared}
  end

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
