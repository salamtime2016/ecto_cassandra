defmodule EctoCassandra.Structure do
  @moduledoc """
  Keep dump of Cassandra DB
  """

  @dump_file "structure.cql"
  @default_cql_version "3.4.4"

  @spec structure_dump(default :: String.t(), config :: Keyword.t()) ::
          {:ok, String.t()}
          | {:error, term}
  def structure_dump(default, config) do
    keyspace = Keyword.get(config, :keyspace)
    path = Path.join(default, @dump_file)

    with {schema, 0} <-
           System.cmd("cqlsh", default_opts(config) ++ ["-e", "DESCRIBE #{keyspace}"]),
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
  def structure_load(default, config) do
    path = Path.join(default, @dump_file)

    case File.exists?(path) do
      true ->
        {_res, _exit_code} = System.cmd("cqlsh", ["-f", path] ++ default_opts(config))
        {:ok, path}

      _ ->
        {:error, :file_not_exists}
    end
  end

  defp default_opts(config) do
    cql_version = Keyword.get(config, :cql_version, @default_cql_version)

    opts =
      case Keyword.get(config, :authentication) do
        {_, [username: username, password: password]} -> ~w(-u #{username} -p #{password})
        _ -> []
      end

    opts ++ ~w(--cqlversion=#{cql_version})
  end
end
