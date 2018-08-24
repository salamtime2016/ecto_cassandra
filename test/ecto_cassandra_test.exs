defmodule EctoCassandraTest do
  use ExUnit.Case
  doctest EctoCassandra

  test "greets the world" do
    assert EctoCassandra.hello() == :world
  end
end
