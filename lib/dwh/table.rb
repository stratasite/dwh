module DWH
  class Table
    attr_reader :physical_name, :schema, :catalog, :columns
    attr_accessor :date_start, :date_end, :row_count

    def initialize(physical_name, schema: nil, catalog: nil, row_count: nil,
      date_start: nil, date_end: nil)
      parts = physical_name.split(".")

      @physical_name = parts.last
      @row_count = row_count
      @date_start = date_start
      @date_end = date_end

      @catalog = catalog
      @schema = schema

      if @catalog.nil? && parts.length > 2
        @catalog = parts.first
      end

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

    def has_catalog_and_schema?
      catalog.present? && schema.present?
    end

    def has_catalog_or_schema?
      catalog.present? || schema.present?
    end

    def to_h
      {
        physical_name: physical_name,
        schema: schema,
        catalog: catalog,
        row_count: row_count,
        date_start: date_start,
        date_end: date_end,
        columns: columns.map(&:to_h)
      }
    end

    def size
      @row_count
    end

    def find_column(name)
      columns.find { |c| c.name.downcase == name.downcase }
    end

    def self.from_hash_or_json(physical_name, metadata)
      if metadata.is_a?(String)
        metadata = JSON.parse(metadata)
      end

      metadata.symbolize_keys!
      table = new(physical_name, row_count: metadata[:row_count],
        date_start: metadata[:date_start], date_end: metadata[:date_end])

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
