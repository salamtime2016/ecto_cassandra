defmodule EctoCassandra.Adapter do
  @moduledoc """
  Ecto 2.x adapter for the Cassandra database
  """

  @behaviour Ecto.Adapter
  @adapter EctoCassandra.Planner
  @storage_adapter EctoCassandra.Storage
  @migration_adapter EctoCassandra.Migration
  @structure_adapter EctoCassandra.Structure

  alias Xandra.Batch

  @doc false
  defmacro __before_compile__(_env), do: :ok

  @doc false
  defdelegate ensure_all_started(repo, type), to: @adapter

  @doc false
  defdelegate child_spec(repo, opts), to: @adapter

  @doc false
  defdelegate prepare(operation, query), to: @adapter

  @doc false
  defdelegate execute(repo, query_meta, query_cache, sources, preprocess, opts),
    to: @adapter

  @doc false
  defdelegate insert(repo, query_meta, sources, on_conflict, returning, opts),
    to: @adapter

  @doc false
  defdelegate insert_all(repo, query_meta, header, rows, on_conflict, returning, opts),
    to: @adapter

  @doc false
  defdelegate update(repo, query_meta, params, filter, autogen, opts), to: @adapter

  @doc false
  defdelegate delete(repo, query_meta, filter, opts), to: @adapter

  @doc false
  @spec transaction(any, any, any) :: nil
  def transaction(_repo, _opts, _fun), do: nil

  @doc false
  @spec in_transaction?(any) :: false
  def in_transaction?(_repo), do: false

  @doc false
  @spec rollback(any, any) :: nil
  def rollback(_repo, _tid), do: nil

  @doc """
  Cassandra batches
  """
  @spec batch([String.t()]) :: any
  def batch(queries) do
    batch =
      Enum.reduce(queries, Batch.new(:logged), fn q, acc ->
        apply(Batch, :add, [acc] ++ [q])
      end)

    Xandra.execute(EctoCassandra.Conn, batch)
  end

  @doc false
  defdelegate autogenerate(type), to: @adapter

  @doc false
  defdelegate loaders(primitive, type), to: @adapter

  @doc false
  defdelegate dumpers(primitive, type), to: @adapter

  @behaviour Ecto.Adapter.Storage

  @doc false
  defdelegate storage_down(opts), to: @storage_adapter

  @doc false
  defdelegate storage_up(opts), to: @storage_adapter

  @behaviour Ecto.Adapter.Migration

  @doc false
  defdelegate execute_ddl(repo, command, options), to: @migration_adapter

  @doc false
  def supports_ddl_transaction?, do: false

  @behaviour Ecto.Adapter.Structure

  @doc false
  defdelegate structure_dump(default, config), to: @structure_adapter

  @doc false
  defdelegate structure_load(default, config), to: @structure_adapter
end
