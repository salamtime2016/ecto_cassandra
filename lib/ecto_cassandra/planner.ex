defmodule EctoCassandra.Planner do
  @moduledoc """
  Ecto Cassandra core planner
  """

  require Logger
  alias Ecto.UUID
  alias EctoCassandra.{Conn, Types}

  @behaviour Ecto.Adapter

  @type t :: EctoCassandra.Adapter.Planner

  @type query_meta :: %{
          prefix: binary | nil,
          sources: tuple,
          assocs: term,
          preloads: term,
          select: term,
          fields: [term]
        }

  @type schema_meta :: %{
          source: source,
          schema: atom,
          context: term,
          autogenerate_id: {atom, :id | :binary_id}
        }

  @type query :: Ecto.Query.t()
  @type source :: {prefix :: binary | nil, table :: binary}
  @type fields :: Keyword.t()
  @type filters :: Keyword.t()
  @type constraints :: Keyword.t()
  @type returning :: [atom]
  @type prepared :: term
  @type cached :: term
  @type process :: (field :: Macro.t(), value :: term, context :: term -> term)
  @type autogenerate_id :: {field :: atom, type :: :id | :binary_id, value :: term} | nil

  @type on_conflict ::
          {:raise, list(), []}
          | {:nothing, list(), [atom]}
          | {query, list(), [atom]}

  @typep repo :: Ecto.Repo.t()
  @typep options :: Keyword.t()

  @doc false
  defmacro __before_compile__(_env), do: :ok

  @spec ensure_all_started(any, type :: :application.restart_type()) ::
          {:ok, [atom]} | {:error, atom}
  def ensure_all_started(_repo, _type) do
    Application.ensure_all_started(:db_connection)
    {:ok, [:db_connection]}
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
  def autogenerate(:embed_id), do: UUID.generate()
  def autogenerate(:binary_id), do: UUID.autogenerate()

  def autogenerate(:id) do
    raise(
      ArgumentError,
      "Cassandra adapter does not support autogenerated :id field type in schema."
    )
  end

  @spec prepare(atom :: :all | :update_all | :delete_all, query) ::
          {:cache, term} | {:nocache, term} | no_return
  def prepare(operation, query) do
    with prepared <- Xandra.prepare!(Conn, EctoCassandra.Query.new(operation, query)),
         do: {:cache, prepared}
  end

  @spec execute(repo, query_meta, query, params :: list, process | nil, options) :: result
        when result: {integer, [[term]] | nil} | no_return,
             query:
               {:nocache, prepared}
               | {:cached, (prepared -> :ok), cached}
               | {:cache, (cached -> :ok), prepared}
  def execute(
        _repo,
        %{sources: {{_table_name, schema}}},
        {:cache, _, prepared},
        sources,
        preprocess,
        _opts
      ) do
    with %Xandra.Page{} = page <- Xandra.execute!(Conn, prepared, sources) do
      pages = Enum.to_list(page)
      {length(pages), Enum.map(pages, &process_row(&1, preprocess, schema.__schema__(:fields)))}
    end
  end

  @spec insert(repo, schema_meta, fields, on_conflict, returning, options) ::
          {:ok, fields}
          | {:invalid, constraints}
          | no_return
  def insert(
        _repo,
        %{schema: schema, source: {_, table}},
        sources,
        _on_conflict,
        _returning,
        _opts
      ) do
    keys = sources |> Keyword.keys() |> Enum.join(", ")
    values = sources |> Enum.map(fn _ -> "?" end) |> Enum.join(", ")
    statement = "INSERT INTO #{table} (#{keys}) VALUES (#{values})"
    prepared_sources = prepare_sources(schema, sources)

    with {:ok, %Xandra.Void{}} <- Xandra.execute(Conn, statement, prepared_sources),
         do: {:ok, []}
  end

  # @spec insert_all(repo, schema_meta, header :: [atom], [fields], on_conflict, returning,
  # options) ::
  #         {integer, [[term]] | nil}
  #         | no_return
  # def insert_all(repo, query_meta, header, rows, on_conflict, returning, opts),
  #   do: raise_not_implemented_error()

  # @spec update(repo, schema_meta, fields, filters, returning, options) ::
  #         {:ok, fields}
  #         | {:invalid, constraints}
  #         | {:error, :stale}
  #         | no_return
  # def update(repo, query_meta, params, filter, autogen, opts), do: raise_not_implemented_error()

  @spec delete(repo, schema_meta, filters, options) ::
          {:ok, fields}
          | {:invalid, constraints}
          | {:error, :stale}
          | no_return
  def delete(_repo, query_meta, _filters, _opts) do
    with {:ok, _} <- Xandra.execute(Conn, Query.new(:delete, query_meta)) do
      {:ok, []}
    else
      err -> {:invalid, err}
    end
  end

  @spec loaders(primitive_type :: Ecto.Type.primitive(), ecto_type :: Ecto.Type.t()) :: [
          (term -> {:ok, term} | :error) | Ecto.Type.t()
        ]
  def loaders(:binary_id, type), do: [fn v -> {:ok, v} end, type]
  def loaders(type, _) when type in ~w(utc_datetime naive_datetime)a, do: [&to_dt/1]
  def loaders(_primitive, type), do: [type]

  @spec dumpers(primitive_type :: Ecto.Type.primitive(), ecto_type :: Ecto.Type.t()) :: [
          (term -> {:ok, term} | :error) | Ecto.Type.t()
        ]
  def dumpers(datetime, _type) when datetime in [:datetime, :utc_datetime, :naive_datetime],
    do: [&to_dt/1]

  def dumpers(_primitive, type), do: [type]

  defp process_row(row, _preprocess, fields) do
    for f <- fields, do: Map.get(row, to_string(f))
  end

  defp prepare_sources(schema, sources) do
    for k <- Keyword.keys(sources), into: %{} do
      ecto_type = schema.__schema__(:type, k)
      {to_string(k), {ecto_type |> Types.to_db() |> to_string, sources[k]}}
    end
  end

  defp to_dt(%NaiveDateTime{} = dt), do: DateTime.from_naive(dt, "Etc/UTC")
  defp to_dt(%DateTime{} = dt), do: {:ok, dt}
  defp to_dt(_), do: :error
end
