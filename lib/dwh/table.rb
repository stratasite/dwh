# frozen_string_literal: true

require_relative "column"

module DWH
  # Container to map to a data warehouse table.
  # If you initialize with a fuly qualified table name
  # , it will automatically create catalog and schema components.
  #
  # This is the object returned from +metadata+ method call of an adapter
  #
  # ==== Examples
  #   Table.new("dwh.public.hello_world_table")
  #
  #   table_stats_instance = adapter.stats("my_table", schema: "dwh")
  #   Table.new("my_table", schema: "dwh", stats: table_stats_instance)
  class Table
    attr_reader :physical_name, :schema, :catalog, :columns, :table_stats

    def initialize(physical_name, schema: nil, catalog: nil, table_stats: nil)
      parts = physical_name.split(".")

      @physical_name = parts.last
      @table_stats = table_stats
      @catalog = catalog
      @schema = schema

      @catalog = parts.first if @catalog.nil? && parts.length > 2

      if @schema.nil?
        if parts.length == 2
          @schema = parts.first
        elsif parts.length > 2
          @schema = parts[1]
        end
      end

      @columns = []
    end

    def <<(column)
      @columns << column
    end

    def fully_qualified_table_name
      [catalog, schema, physical_name].compact.join(".")
    end

    def fully_qualified_schema_name
      [catalog, schema].compact.join(".")
    end

    def catalog_and_schema?
      catalog && schema
    end

    def catalog_or_schema?
      catalog || schema
    end

    def stats
      @table_stats
    end

    def to_h
      {
        physical_name: physical_name,
        schema: schema,
        catalog: catalog,
        columns: columns.map(&:to_h),
        stats: table_stats&.to_h
      }
    end

    def size
      @table_stats&.row_count || 0
    end

    def find_column(name)
      columns.find { |c| c.name.downcase == name.downcase }
    end

    def self.from_hash_or_json(physical_name, metadata)
      metadata = JSON.parse(metadata) if metadata.is_a?(String)
      metadata.symbolize_keys!

      stats = TableStats.new(**metadata[:stats].symbolize_keys) if metadata.key?(:stats)
      table = new(physical_name, table_stats: stats)

      metadata[:columns]&.each do |col|
        col.symbolize_keys!
        table << Column.new(
          name: col[:name],
          data_type: col[:data_type],
          precision: col[:precision],
          scale: col[:scale],
          max_char_length: col[:max_char_length],
          schema_type: col[:schema_type]
        )
      end

      table
    end
  end
end
