defmodule EctoCassandra.Migration do
  @moduledoc """
  Implement Ecto migrations
  """

  @spec execute_ddl(
          repo :: Ecto.Repo.t(),
          Ecto.Adapters.Migration.command(),
          options :: Keyword.t()
        ) :: :ok | no_return
  def execute_ddl(repo, command, options) do
    IO.inspect(repo)
    IO.inspect(command)
    IO.inspect(options)
  end
end
