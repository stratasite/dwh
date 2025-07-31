module DWH
  module Adapters
    class Trino < Adapter
      define_config :host, required: true, message: 'server host ip address or domain name'
      define_config :port, required: false, default: 8080, message: 'port to connect to'
      define_config :ssl, required: false, default: false, message: 'use ssl?'
      define_config :catalog, required: true, message: 'catalog to connect to'
      define_config :schema, required: false, message: 'default schema'
      define_config :username, required: true, message: 'connection username'
      define_config :password, required: false, default: nil, message: 'connection password'
      define_config :query_timeout, required: false, default: 3600, message: 'query execution timeout in seconds'
      define_config :source, required: false, default: 'dwh-gem', message: 'source param'

      def connection
        return @connection if @connection

        properties = {
          server: "#{config[:host]}:#{config[:port]}",
          schema: config[:schema],
          catalog: config[:catalog],
          user: config[:username],
          password: config[:password],
          query_timeout: config[:query_timeout],
          source: config[:source]
        }.merge(extra_connection_params)

        @connection = ::Trino::Client.new(properties)
      end

      def close
        connection.close if @connection
      end

      def set_user_context(user: nil, password: nil, timeout: nil)
        connection.instance_variable_get(:@options)[:query_timeout] = timeout if timeout
        connection.instance_variable_get(:@options)[:user] = user if user
        connection.instance_variable_get(:@options)[:password] = password if password
      end

      def reset_user_context
        connection.instance_variable_get(:@options)[:query_timeout] = config[:query_timeout]
        connection.instance_variable_get(:@options)[:user] = config[:username]
        connection.instance_variable_get(:@options)[:password] = config[:password]
      end

      def tables(schema: nil, catalog: nil)
        query = ['SHOW TABLES']
        query << 'FROM' if catalog || schema

        if catalog && schema
          query << "#{catalog}.#{schema}"
        else
          query << catalog
          query << schema
        end

        res = execute(query.compact.join(' '), retries: 1)

        res.flatten
      end

      def table?(table, catalog: nil, schema: nil)
        db_table = Table.new(table, catalog: catalog, schema: schema)

        query = ['SHOW TABLES']

        if db_table.has_catalog_or_schema?
          query << 'FROM'
          query << db_table.fully_qualified_schema_name
        end
        query << "LIKE '#{db_table.physical_name}'"

        res = execute(query.compact.join(' '), retries: 1)
        if !res.empty?
          res.flatten.include?(db_table.physical_name)
        else
          false
        end
      end

      def stats(table, date_column: nil, catalog: nil, schema: nil)
        db_table = Table.new(table, catalog: catalog, schema: schema)
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM #{db_table.fully_qualified_table_name}
        SQL

        result = execute(sql, retries: 1)
        row = result[0]

        {
          date_start: row[1],
          date_end: row[2],
          row_count: row[0]
        }
      end

      def metadata(table, catalog: nil, schema: nil)
        db_table = Table.new table, schema: schema, catalog: catalog
        sql = "SHOW COLUMNS FROM #{db_table.fully_qualified_table_name}"

        cols = execute(sql, retries: 1)

        cols.each do |col|
          dt = col[1].start_with?('row(') ? 'struct' : col[1]
          db_table << Column.new(
            name: col[0],
            data_type: dt
          )
        end

        db_table
      end

      def schema?
        config.key?(:schema)
      end

      def execute(sql, retries: 2)
        result = with_debug(sql) do
          with_retry(retries) { connection.run(sql) }
        end

        result[1]
      end

      def execute_stream(sql, io, memory_row_limit: 20_000, stats: nil)
        stats = validate_and_reset_stats(stats)

        with_debug(sql) do
          with_retry(3) do
            connection.query(sql) do |result|
              result.each_row do |row|
                update_stats(stats, row, memory_row_limit)
                io << CSV.generate_line(row)
              end
            end
          end
        end

        io.rewind
        io
      end
    end
  end
end
