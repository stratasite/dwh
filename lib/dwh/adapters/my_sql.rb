
# TODO: this is barely implemented only connection and tables are kind of implemented
module DWH
  module Adapters
    class MySql < Adapter
      define_config :host, required: true, message: "server host ip address or domain name"
      define_config :port, required: false, default: 3306, message: "port to connect to"
      define_config :database, required: true, message: "name of database to connect to"
      define_config :username, required: true, message: "connection username"
      define_config :password, required: false, default: nil, message: "connection password"
      define_config :query_timeout, required: false, default: 3600, message: "query execution timeout in seconds"

      def connection
        return @connection if @connection

        properties = {
          # Connection Settings
          host: config[:host],
          username: config[:username],
          password: config[:password],
          port: 3306,
          database: config[:database],

          # Timeout Settings
          connect_timeout: 10,
          read_timeout: config[:query_timeout]

        }.merge(extra_connection_params)

        @connection = Mysql2::Client.new(properties)
      end

      def tables
        query = """
                  SELECT
                    t.table_name,
                    t.table_type,
                    t.engine,
                    t.table_rows,
                    t.avg_row_length,
                    ROUND((t.data_length + t.index_length) / 1024 / 1024, 2) as size_mb,
                    t.create_time,
                    t.update_time,
                    t.table_comment
                  FROM information_schema.tables t
                  WHERE t.table_schema = '#{@connection.escape(config[:database])}'
                  ORDER BY t.table_name
        """

        connection.exec(query)
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
        db_table    = Table.new table, schema: schema

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
            name:               col["column_name"],
            data_type:          col["data_type"],
            precision:          col["numeric_precision"],
            scale:              col["numeric_scale"],
            max_char_length:    col["character_maximum_length"]
          )
        end

        db_table
      end

      def execute(sql)
        with_debug(sql) { connection.query(sql) }
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

      private

      def qualified_schema_name
        @qualified_schema_name ||= config[:schema].split(",").map { |s| "'#{s}'" }.join(",")
      end
    end
  end
end
