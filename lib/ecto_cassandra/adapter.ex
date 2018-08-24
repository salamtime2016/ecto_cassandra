defmodule EctoCassandra.Adapter do
  @moduledoc """
  Ecto 2.x adapter for the Cassandra database
  """

  @behaviour Ecto.Adapter
  @adapter_implementation EctoCassandra.Planner

  @doc false
  defmacro __before_compile__(_env), do: :ok

  @doc false
  defdelegate ensure_all_started(repo, type), to: @adapter_implementation

  @doc false
  defdelegate child_spec(repo, opts), to: @adapter_implementation

  @doc false
  defdelegate prepare(operation, query), to: @adapter_implementation

  @doc false
  defdelegate execute(repo, query_meta, query_cache, sources, preprocess, opts),
    to: @adapter_implementation

  @doc false
  defdelegate insert(repo, query_meta, sources, on_conflict, returning, opts),
    to: @adapter_implementation

  @doc false
  defdelegate insert_all(repo, query_meta, header, rows, on_conflict, returning, opts),
    to: @adapter_implementation

  @doc false
  defdelegate update(repo, query_meta, params, filter, autogen, opts), to: @adapter_implementation

  @doc false
  defdelegate delete(repo, query_meta, filter, opts), to: @adapter_implementation

  @doc false
  defdelegate transaction(repo, opts, fun), to: @adapter_implementation

  @doc false
  def in_transaction?(repo), do: false

  @doc false
  def rollback(repo, tid), do: nil

  @doc false
  defdelegate autogenerate(type), to: @adapter_implementation

  @doc false
  defdelegate loaders(primitive, type), to: @adapter_implementation

  @doc false
  defdelegate dumpers(primitive, type), to: @adapter_implementation

  @doc false
  def supports_ddl_transaction?, do: false
end
