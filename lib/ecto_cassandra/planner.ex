defmodule EctoCassandra.Planner do
  @moduledoc """
  Ecto Cassandra core planner
  """

  require Logger
  alias Ecto.UUID
  alias EctoCassandra.{Conn, Query, Types}

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

  @spec ensure_all_started(any, type :: Application.restart_type()) ::
          {:error, atom} | {:ok, [atom()]}
  def ensure_all_started(_repo, _type) do
    with {:ok, apps} <- Application.ensure_all_started(:db_connection) do
      {:ok, apps}
    else
      {:error, {atom, _}} -> {:error, atom}
    end
  end

  @spec child_spec(any, keyword) :: Supervisor.Spec.spec()
  def child_spec(_repo, opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)

    opts =
      Keyword.merge(opts,
        name: EctoCassandra.Conn,
        after_connect: &Xandra.execute(&1, "USE #{keyspace}")
      )

    Supervisor.Spec.worker(Xandra, [opts], restart: :permanent)
  end

  @doc """
  Automatically generate next ID for binary keys, leave sequence keys empty for generation on insert.
  """
  @spec autogenerate(:binary_id | :embed_id) :: <<_::288>>
  def autogenerate(:embed_id) do
    UUID.generate()
  end

  def autogenerate(:binary_id) do
    UUID.autogenerate()
  end

  def autogenerate(_) do
    raise(
      ArgumentError,
      "Cassandra adapter does not support autogenerated :id field type in schema."
    )
  end

  @spec prepare(atom :: :all | :update_all | :delete_all, query) ::
          {:cache, Xandra.Prepared.t()} | no_return
  def prepare(operation, query) do
    with prepared <- Xandra.prepare!(Conn, EctoCassandra.Query.new(operation, query)),
         do: {:cache, prepared}
  end

  @spec execute(repo, query_meta, query, params :: list, process | nil, options) :: result
        when result: {integer, [[term]] | nil} | no_return,
             query:
               {:cached, (prepared -> :ok), cached}
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
    statement = Query.new(insert: {table, keys, values})
    prepared_sources = prepare_sources(schema, sources)

    with {:ok, %Xandra.Void{}} <- Xandra.execute(Conn, statement, prepared_sources),
         do: {:ok, []}
  end

  @spec insert_all(repo, schema_meta, header :: [atom], [fields], any, returning, options) ::
          {integer, [[term]] | nil}
          | no_return
  def insert_all(_repo, %{source: {_, table}}, header, rows, _on_conflict, returning, _opts) do
    :ok = Logger.debug(fn -> {returning, header} end)
    statement = "INSERT INTO #{table}"

    with %Xandra.Page{} = page <- Xandra.execute!(Conn, statement, rows) do
      pages = Enum.to_list(page)
      {length(pages), pages}
    end
  end

  @spec update(repo, schema_meta, fields, filters, returning, options) ::
          {:ok, fields}
          | {:invalid, constraints}
          | no_return
  def update(_repo, %{source: {nil, table_name}, schema: schema}, params, filter, _gen, _opts) do
    statement = Query.new(:update, {table_name, params, filter})
    sources = prepare_sources(schema, params)

    with {:ok, %Xandra.Void{}} <- Xandra.execute(Conn, statement, sources) do
      {:ok, []}
    else
      {:error, any} -> {:invalid, any}
    end
  end

  @spec delete(repo, schema_meta, filters, options) ::
          {:ok, fields}
          | {:invalid, constraints}
          | no_return
  def delete(_repo, %{source: {nil, table_name}}, filters, _opts) do
    with %Xandra.Void{} <- Xandra.execute!(Conn, Query.new(:delete, {table_name, filters})) do
      {:ok, []}
    else
      err -> {:invalid, err}
    end
  end

  @spec loaders(primitive_type :: Ecto.Type.primitive(), ecto_type :: Ecto.Type.t()) :: [
          (term -> {:ok, term} | :error) | Ecto.Type.t()
        ]
  def loaders(:binary_id, type) do
    [
      &case UUID.cast(&1) do
        {:ok, uuid} -> {:ok, uuid}
        _ -> {:ok, &1}
      end,
      type
    ]
  end

  def loaders(type, _) when type in ~w(utc_datetime naive_datetime)a, do: [&to_dt/1]
  def loaders(_primitive, type), do: [type]

  @spec dumpers(primitive_type :: Ecto.Type.primitive(), ecto_type :: Ecto.Type.t()) :: [
          (term -> {:ok, term} | :error) | Ecto.Type.t()
        ]
  def dumpers(:binary_id, type), do: [type]

  def dumpers(datetime, _type) when datetime in [:datetime, :utc_datetime, :naive_datetime],
    do: [&to_dt/1]

  def dumpers(_primitive, type), do: [type]

  defp process_row(row, preprocess, fields) do
    fields |> Enum.map(fn f -> Map.get(row, to_string(f)) end) |> preprocess.()
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
