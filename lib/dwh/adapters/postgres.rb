module DWH
  module Adapters
    class Postgres < Adapter
      define_config :host, required: true, message: "server host ip address or domain name"
      define_config :port, required: false, default: 5432, message: "port to connect to"
      define_config :database, required: true, message: "name of database to connect to"
      define_config :username, required: true, message: "connection username"
      define_config :password, required: false, default: nil, message: "connection password"
      define_config :query_timeout, required: false, default: 3600, message: "query execution timeout in seconds"

      def connection
        return @connection if @connection

        properties = {
          host: config[:host],
          port: config[:port],
          dbname: config[:database],
          user: config[:username],
          password: config[:password]
        }.merge(extra_connection_params)

        properties[:options] = "#{properties[:options]} -c statement_timeout=#{config[:query_timeout]}s"

        @connection = PG.connect(properties)

        if schema?
          # this could be comma separated list
          @connection.exec("SET search_path TO #{config[:schema]}")
        end

        @connection
      end

      def tables
        sql = if schema?
          <<-SQL
                        SELECT table_schema || '.' || table_name#{" "}
                        FROM information_schema.tables
                        WHERE table_schema in (#{qualified_schema_name})
          SQL
        else
          <<-SQL
                        SELECT table_name#{" "}
                        FROM information_schema.tables
          SQL
        end

        result = connection.exec(sql)
        result.values.flatten
      end

      def stats(table, date_column: nil, catalog: nil, schema: nil)
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM "#{table}"
        SQL

        result = connection.exec(sql)

        {
          date_start: result.first["date_start"],
          date_end: result.first["date_end"],
          row_count: result.first["row_count"]
        }
      end

      def metadata(table, catalog: nil, schema: nil)
        db_table = Table.new table, schema: schema

        schema_where = ""
        if db_table.schema.present?
          schema_where = "AND table_schema = '#{db_table.schema}'"
        elsif schema?
          schema_where = "AND table_schema in (#{qualified_schema_name})"
        end

        sql = <<-SQL
                    SELECT column_name, data_type, character_maximum_length, numeric_precision,numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = '#{db_table.physical_name}'
                    #{schema_where}
        SQL

        cols = execute(sql, "object")
        cols.each do |col|
          db_table << Column.new(
            name: col["column_name"],
            data_type: col["data_type"],
            precision: col["numeric_precision"],
            scale: col["numeric_scale"],
            max_char_length: col["character_maximum_length"]
          )
        end

        db_table
      end

      def schema?
        config[:schema].present?
      end

      def execute(sql, format = "array")
        result = with_debug(sql) { connection.exec(sql) }

        if result == "array"
          return result.values
        end

        result
      end

      def execute_stream(sql, io, memory_row_limit: 20000, stats: nil)
        stats = validate_and_reset_stats(stats)

        with_debug(sql) do
          connection.exec(sql) do |result|
            result.each_row do |row|
              update_stats(stats, row, memory_row_limit)
              io.write(CSV.generate_line(row))
            end
          end
        end

        io.rewind
        io
      end

      # Need to override default add method
      # since postgres doesn't support quarter as an
      # interval.
      # TODO: Need to check if other db's also do not
      # support Quarter as an interval unit.
      def date_add(unit, val, exp)
        if unit.downcase.strip == "quarter"
          unit = "months"
          val = val.to_i * 3
        end
        gsk(:date_add)
          .gsub("@UNIT", unit)
          .gsub("@VAL", val.to_s)
          .gsub("@EXP", exp)
      end

      private

      def qualified_schema_name
        @qualified_schema_name ||= config[:schema].split(",").map { |s| "'#{s}'" }.join(",")
      end
    end
  end
end
