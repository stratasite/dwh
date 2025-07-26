# frozen_string_literal: true

require "test_helper"

class TestTableAndColumn < Minitest::Test

  def test_column_initialization
    column = DWH::Column.new(
      name: "TEST_NAME",
      data_type: "VARCHAR",
      precision: 50,
      scale: 0,
      schema_type: "DIMENSION",
      max_char_length: 100
    )

    assert_equal "test_name", column.name
    assert_equal "varchar", column.data_type
    assert_equal 50, column.precision
    assert_equal 0, column.scale
    assert_equal "dimension", column.schema_type
    assert_equal 100, column.max_char_length
  end

  def test_column_dim_and_measure
    dim_column = DWH::Column.new(name: "category", data_type: "varchar", schema_type: "dimension")
    measure_column = DWH::Column.new(name: "amount", data_type: "decimal", schema_type: "measure")

    assert dim_column.dim?
    refute dim_column.measure?
    assert measure_column.measure?
    refute measure_column.dim?
  end

  def test_column_namify
    column = DWH::Column.new(name: "customer_id", data_type: "int")
    assert_equal "Customer ID", column.namify

    column = DWH::Column.new(name: "product_desc", data_type: "varchar")
    assert_equal "Product Description", column.namify
    
    column = DWH::Column.new(name: "tide_id", data_type: "varchar")
    assert_equal "Tide ID", column.namify
  end

  def test_column_normalized_data_type
    varchar_column = DWH::Column.new(name: "name", data_type: "varchar")
    assert_equal "string", varchar_column.normalized_data_type

    date_column = DWH::Column.new(name: "created_at", data_type: "date")
    assert_equal "date", date_column.normalized_data_type

    datetime_column = DWH::Column.new(name: "updated_at", data_type: "datetime")
    assert_equal "date_time", datetime_column.normalized_data_type

    int_column = DWH::Column.new(name: "count", data_type: "int")
    assert_equal "integer", int_column.normalized_data_type

    bigint_column = DWH::Column.new(name: "large_count", data_type: "bigint")
    assert_equal "bigint", bigint_column.normalized_data_type

    decimal_column = DWH::Column.new(name: "price", data_type: "decimal")
    assert_equal "decimal", decimal_column.normalized_data_type

    boolean_column = DWH::Column.new(name: "active", data_type: "boolean")
    assert_equal "boolean", boolean_column.normalized_data_type

    number_bigint_column = DWH::Column.new(name: "id", data_type: "number", precision: 38, scale: 0)
    assert_equal "bigint", number_bigint_column.normalized_data_type

    number_decimal_column = DWH::Column.new(name: "amount", data_type: "number", precision: 10, scale: 2)
    assert_equal "decimal", number_decimal_column.normalized_data_type

    number_int_column = DWH::Column.new(name: "count", data_type: "number", precision: 5, scale: 0)
    assert_equal "integer", number_int_column.normalized_data_type

    unknown_column = DWH::Column.new(name: "unknown", data_type: "unknown_type")
    assert_equal "string", unknown_column.normalized_data_type
  end

  def test_column_to_h
    column = DWH::Column.new(
      name: "test_col",
      data_type: "varchar",
      precision: 50,
      scale: 0,
      schema_type: "dimension",
      max_char_length: 100
    )

    hash = column.to_h
    assert_equal "test_col", hash[:name]
    assert_equal "varchar", hash[:data_type]
    assert_equal 50, hash[:precision]
    assert_equal 0, hash[:scale]
    assert_equal "dimension", hash[:schema_type]
    assert_equal 100, hash[:max_char_length]
  end

  def test_column_to_s
    column = DWH::Column.new(name: "test_col", data_type: "varchar")
    assert_equal "<Column:test_col:varchar>", column.to_s
  end

  def test_table_initialization_simple
    table = DWH::Table.new("users")
    assert_equal "users", table.physical_name
    assert_nil table.schema
    assert_nil table.catalog
    assert_equal [], table.columns
  end

  def test_table_initialization_with_schema
    table = DWH::Table.new("public.users")
    assert_equal "users", table.physical_name
    assert_equal "public", table.schema
    assert_nil table.catalog
  end

  def test_table_initialization_with_catalog_and_schema
    table = DWH::Table.new("db.public.users")
    assert_equal "users", table.physical_name
    assert_equal "public", table.schema
    assert_equal "db", table.catalog
  end

  def test_table_initialization_with_parameters
    table = DWH::Table.new(
      "users",
      schema: "analytics",
      catalog: "warehouse",
      row_count: 1000,
      date_start: Date.new(2023, 1, 1),
      date_end: Date.new(2023, 12, 31)
    )

    assert_equal "users", table.physical_name
    assert_equal "analytics", table.schema
    assert_equal "warehouse", table.catalog
    assert_equal 1000, table.row_count
    assert_equal Date.new(2023, 1, 1), table.date_start
    assert_equal Date.new(2023, 12, 31), table.date_end
  end

  def test_table_add_column
    table = DWH::Table.new("users")
    column = DWH::Column.new(name: "id", data_type: "int")
    
    table << column
    assert_equal 1, table.columns.length
    assert_equal column, table.columns.first
  end

  def test_table_fully_qualified_table_name
    table = DWH::Table.new("db.public.users")
    assert_equal "db.public.users", table.fully_qualified_table_name

    table = DWH::Table.new("public.users")
    assert_equal "public.users", table.fully_qualified_table_name

    table = DWH::Table.new("users")
    assert_equal "users", table.fully_qualified_table_name
  end

  def test_table_fully_qualified_schema_name
    table = DWH::Table.new("db.public.users")
    assert_equal "db.public", table.fully_qualified_schema_name

    table = DWH::Table.new("public.users")
    assert_equal "public", table.fully_qualified_schema_name

    table = DWH::Table.new("users")
    assert_equal "", table.fully_qualified_schema_name
  end

  def test_table_has_catalog_and_schema
    table = DWH::Table.new("db.public.users")
    assert table.has_catalog_and_schema?

    table = DWH::Table.new("public.users")
    refute table.has_catalog_and_schema?

    table = DWH::Table.new("users")
    refute table.has_catalog_and_schema?
  end

  def test_table_has_catalog_or_schema
    table = DWH::Table.new("db.public.users")
    assert table.has_catalog_or_schema?

    table = DWH::Table.new("public.users")
    assert table.has_catalog_or_schema?

    table = DWH::Table.new("users")
    refute table.has_catalog_or_schema?
  end

  def test_table_size
    table = DWH::Table.new("users", row_count: 500)
    assert_equal 500, table.size
  end

  def test_table_find_column
    table = DWH::Table.new("users")
    column1 = DWH::Column.new(name: "ID", data_type: "int")
    column2 = DWH::Column.new(name: "NAME", data_type: "varchar")
    
    table << column1
    table << column2

    found_column = table.find_column("id")
    assert_equal column1, found_column

    found_column = table.find_column("ID")
    assert_equal column1, found_column

    found_column = table.find_column("nonexistent")
    assert_nil found_column
  end

  def test_table_to_h
    table = DWH::Table.new("users", row_count: 100)
    column = DWH::Column.new(name: "id", data_type: "int")
    table << column

    hash = table.to_h
    assert_equal "users", hash[:physical_name]
    assert_equal 100, hash[:row_count]
    assert_equal 1, hash[:columns].length
    assert_equal "id", hash[:columns].first[:name]
  end

  def test_table_from_hash_or_json
    metadata = {
      row_count: 1000,
      date_start: "2023-01-01",
      date_end: "2023-12-31",
      columns: [
        {
          name: "id",
          data_type: "int",
          precision: nil,
          scale: nil,
          max_char_length: nil,
          schema_type: "dimension"
        },
        {
          name: "name",
          data_type: "varchar",
          precision: nil,
          scale: nil,
          max_char_length: 100,
          schema_type: "dimension"
        }
      ]
    }

    table = DWH::Table.from_hash_or_json("users", metadata)
    assert_equal "users", table.physical_name
    assert_equal 1000, table.row_count
    assert_equal "2023-01-01", table.date_start
    assert_equal "2023-12-31", table.date_end
    assert_equal 2, table.columns.length
    assert_equal "id", table.columns[0].name
    assert_equal "name", table.columns[1].name
  end

  def test_table_from_json_string
    json_string = '{"row_count": 500, "columns": [{"name": "id", "data_type": "int", "schema_type": "dimension"}]}'
    table = DWH::Table.from_hash_or_json("test_table", json_string)
    
    assert_equal "test_table", table.physical_name
    assert_equal 500, table.row_count
    assert_equal 1, table.columns.length
    assert_equal "id", table.columns.first.name
  end

  def test_table_from_hash_with_string_keys
    metadata = {
      "row_count" => 750,
      "date_start" => "2023-06-01",
      "date_end" => "2023-06-30",
      "columns" => [
        {
          "name" => "user_id",
          "data_type" => "bigint",
          "precision" => nil,
          "scale" => nil,
          "max_char_length" => nil,
          "schema_type" => "dimension"
        },
        {
          "name" => "email",
          "data_type" => "varchar",
          "precision" => nil,
          "scale" => nil,
          "max_char_length" => 255,
          "schema_type" => "dimension"
        }
      ]
    }

    table = DWH::Table.from_hash_or_json("user_accounts", metadata)
    assert_equal "user_accounts", table.physical_name
    assert_equal 750, table.row_count
    assert_equal "2023-06-01", table.date_start
    assert_equal "2023-06-30", table.date_end
    assert_equal 2, table.columns.length
    assert_equal "user_id", table.columns[0].name
    assert_equal "bigint", table.columns[0].data_type
    assert_equal "email", table.columns[1].name
    assert_equal "varchar", table.columns[1].data_type
    assert_equal 255, table.columns[1].max_char_length
  end
end
