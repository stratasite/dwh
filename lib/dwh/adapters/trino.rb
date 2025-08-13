module DWH
  module Adapters
    # Trino adapter. This should work for Presto as well.
    # This adapter requires the {https://github.com/treasure-data/trino-client-ruby trino-client-ruby} gem.
    #
    # Create adatper instances using {DWH::Factory#create DWH.create}.
    #
    # @example Basic connection with required only options
    #   DWH.create(:trino, {host: 'localhost', catalog: 'native', username: 'Ajo'})
    #
    # @example Connect with extra http headers
    #   DWH.create(:trino, {host: 'localhost', port: 8080,
    #     catalog: 'native', username: 'Ajo',
    #     extra_connection_params: {
    #       http_headers: {
    #         'X-Trino-User' => 'True User Name',
    #         'X-Forwarded-Request' => '<request passed down from client'
    #       }
    #     }
    #     })
    class Trino < Adapter
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: true, default: 8080, message: 'port to connect to'
      config :ssl, Boolean, required: false, default: false, message: 'use ssl?'
      config :catalog, String, required: true, message: 'catalog to connect to'
      config :schema, String, required: false, message: 'default schema'
      config :username, String, required: true, message: 'connection username'
      config :password, String, required: false, default: nil, message: 'connection password'
      config :query_timeout, Integer, required: false, default: 3600, message: 'query execution timeout in seconds'
      config :client_name, String, required: false, default: 'DWH Ruby Gem', message: 'client name for tracking'

      def connection
        return @connection if @connection

        _ssl = config[:ssl] ? { verify: false } : config[:ssl]

        properties = {
          server: "#{config[:host]}:#{config[:port]}",
          ssl: _ssl,
          schema: config[:schema],
          catalog: config[:catalog],
          user: config[:username],
          password: config[:password],
          query_timeout: config[:query_timeout],
          source: config[:client_name]
        }.merge(extra_connection_params)

        @connection = ::Trino::Client.new(properties)
      rescue StandardError => e
        raise ConfigError, e.message
      end

      # (see Adapter#test_conection)
      def test_connection(raise_exception: false)
        connection.run('select 1')
        true
      rescue ::Trino::Client::TrinoHttpError, Faraday::ConnectionFailed => e
        raise ConnectionError, e.message if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables(**qualifiers)
        catalog, schema = qualifiers.values_at(:catalog, :schema)
        query = ['SHOW TABLES']
        query << 'FROM' if catalog || schema

        if catalog && schema
          query << "#{catalog}.#{schema}"
        else
          query << catalog
          query << schema
        end

        rows = execute(query.compact.join(' '), retries: 1)
        rows.flatten
      end

      # (see Adapter#table?)
      def table?(table, **qualifiers)
        db_table = Table.new(table, **qualifiers)

        query = ['SHOW TABLES']

        if db_table.catalog_or_schema?
          query << 'FROM'
          query << db_table.fully_qualified_schema_name
        end
        query << "LIKE '#{db_table.physical_name}'"

        rows = execute(query.compact.join(' '), retries: 1)
        !rows.empty?
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        db_table = Table.new(table, **qualifiers)
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM #{db_table.fully_qualified_table_name}
        SQL

        rows = execute(sql, retries: 1)
        row = rows[0]

        TableStats.new(
          date_start: row[1],
          date_end: row[2],
          row_count: row[0]
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, **qualifiers
        sql = "SHOW COLUMNS FROM #{db_table.fully_qualified_table_name}"

        _, cols = execute(sql, format: :native, retries: 1)

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

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 2)
        result = with_debug(sql) do
          with_retry(retries) do
            if format == :object
              connection.run_with_names(sql)
            else
              connection.run(sql)
            end
          end
        end

        case format
        when :native
          result
        when :csv
          result_to_csv(result)
        when :array
          result[1]
        when :object
          result
        else
          raise UnsupportedCapability, "Unknown format type: #{format}. Should be :native, :array, :object, or :csv"
        end
      rescue ::Trino::Client::TrinoQueryError => e
        raise ExecutionError, e.message
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 1)
        with_debug(sql) do
          with_retry(retries) do
            connection.query(sql) do |result|
              result.each_row do |row|
                stats << row if stats
                io << CSV.generate_line(row)
              end
            end
          end
        end

        io.rewind
        io
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        with_debug(sql) do
          connection.query(sql) do |result|
            result.each_row(&block)
          end
        end
      end

      def valid_config?
        super
        require 'trino-client'
      rescue LoadError
        raise ConfigError, "Required 'trino-client' gem missing. Please add it to your Gemfile."
      end

      private

      def result_to_csv(result)
        columns, rows = result
        CSV.generate do |csv|
          csv << columns.map(&:name)
          rows.each do |row|
            csv << row
          end
        end
      end
    end
  end
end
