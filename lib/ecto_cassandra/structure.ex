defmodule EctoCassandra.Structure do
  @moduledoc """
  Keep dump of Cassandra DB
  """

  @dump_file "structure.cql"

  @spec structure_dump(default :: String.t(), config :: Keyword.t()) ::
          {:ok, String.t()}
          | {:error, term}
  def structure_dump(default, config) do
    keyspace = Keyword.get(config, :keyspace)
    path = Path.join(default, @dump_file)

    with {schema, 0} <- System.cmd("cqlsh", ["-e", "DESCRIBE #{keyspace}"]),
         :ok <- File.write(path, schema) do
      {:ok, path}
    else
      {err, _} -> {:error, err}
      err -> err
    end
  end

  @spec structure_load(default :: String.t(), config :: Keyword.t()) ::
          {:ok, String.t()}
          | {:error, term}
  def structure_load(default, _config) do
    path = Path.join(default, @dump_file)

    with {_res, 0} <- System.cmd("cqlsh", ["-f", path]) do
      {:ok, path}
    else
      {err, _} -> {:error, err}
    end
  end
end
