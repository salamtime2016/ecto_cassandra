defmodule EctoCassandra.QueryTest do
  @moduledoc """
  Test EctoCassandra.Query
  """

  alias Ecto.Query, as: EctoQuery
  alias EctoCassandra.Query
  use ExUnit.Case, async: true

  describe "CREATE INDEX" do
    test "returns index creation statement" do
      assert "CREATE INDEX index ON tablo (column)" =
               Query.new(create_index: {"tablo", ["column"], "index"})
    end
  end

  describe "DROP TABLE" do
    test "returns drop table statement" do
      assert "DROP TABLE tablo" = Query.new(drop: "tablo")
    end
  end

  describe "DROP INDEX" do
    test "returns drop index statement" do
      assert "DROP INDEX index" = Query.new(drop_index: "index")
    end
  end

  describe "SELECT" do
    test "returns select statement" do
      assert "SELECT * FROM tablo" =
               Query.new(all: %EctoQuery{from: {"tablo", "tablo"}, wheres: []})
    end
  end
end
