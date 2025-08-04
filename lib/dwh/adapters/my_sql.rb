module DWH
  module Adapters
    # MySql Adapter. To use this adapter make sure you have the
    # {https://github.com/brianmario/mysql2 MySql2 Gem} installed. You
    # can also pass additional connection properties via {Adapter#extra_connection_params}
    # config property.
    #
    # MySql concept of database maps to schema in this adapter. This is only important
    # for the metadata methods where you want to pull up tables from a different
    # database   (aka schema).
    class MySql < Adapter
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: false, default: 3306, message: 'port to connect to'
      config :database, String, required: true, message: 'name of database to connect to'
      config :username, String, required: true, message: 'connection username'
      config :password, String, required: false, default: nil, message: 'connection password'
      config :query_timeout, Integer, required: false, default: 3600, message: 'query execution timeout in seconds'
      config :ssl, Boolean, required: false, default: false, message: 'use ssl'
      config :client_name, String, required: false, default: 'DWH Ruby Gem', message: 'The name of the connecting app'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        set_default_ssl_mode_if_needed

        properties = {
          # Connection Settings
          host: config[:host],
          username: config[:username],
          password: config[:password],
          port: 3306,
          database: config[:database],

          # Timeout Settings
          connect_timeout: 10,
          read_timeout: config[:query_timeout],
          connect_attrs: {
            program: config[:client_name]
          }
        }.merge(extra_connection_params)

        @connection = Mysql2::Client.new(properties)
      rescue StandardError => e
        raise ConfigError, e.message
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        connection
        true
      rescue StandardError => e
        raise ConnectionError, e.message if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables(**qualifiers)
        schema = qualifiers[:schema] || config[:database]
        query = "
                  SELECT
                    t.table_name,
                    t.table_type,
                    t.engine,
                    t.table_rows
                  FROM information_schema.tables t
                  WHERE t.table_schema = '#{schema}'
                  ORDER BY t.table_name
        "

        res = connection.query(query, as: :array)
        res.to_a
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        table = "#{qualifiers[:schema]}.#{table}" if qualifiers[:schema]
        sql = <<-SQL
                    SELECT count(*) row_count
                        #{date_column.nil? ? nil : ", min(#{date_column}) date_start"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) date_end"}
                    FROM #{table}
        SQL

        result = connection.query(sql)

        TableStats.new(
          row_count: result.first['row_count'],
          date_start: result.first['date_start'],
          date_end: result.first['date_end']
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, schema: qualifiers[:schema]
        schema_where = db_table.schema ? " AND table_schema = '#{db_table.schema}'" : ''

        sql = <<-SQL
        SELECT column_name, data_type, character_maximum_length, numeric_precision,numeric_scale
        FROM information_schema.columns
        WHERE lower(table_name) = lower('#{db_table.physical_name}')
        #{schema_where}
        SQL

        cols = execute(sql, format: :object)
        cols.each do |col|
          db_table << Column.new(
            name: col['COLUMN_NAME'],
            data_type: col['DATA_TYPE'],
            precision: col['NUMERIC_PRECISION'],
            scale: col['NUMERIC_SCALE'],
            max_char_length: col['CHARACTER_MAXIMUM_LENGTH']
          )
        end

        db_table
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        begin
          as_param = %i[array object].include?(format) ? format : :array
          result = with_debug(sql) { with_retry(retries) { connection.query(sql, as: as_param) } }
        rescue StandardError => e
          raise ExecutionError, e.message
        end

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array, :object
          result.to_a
        when :csv
          result_to_csv(result)
        when :native
          result
        else
          raise UnsupportedCapability, "Unsupported format: #{format} for this #{name}"
        end
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 0)
        with_debug(sql) do
          with_retry(retries) do
            result = connection.query(sql, stream: true, as: :array, cache_rows: false)
            result.each do |row|
              io.write(CSV.generate_line(row))
              stats << row if stats
            end
          end
        end

        io.rewind
        io
      rescue StandardError => e
        raise ExecutionError, e.message
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        with_debug(sql) do
          result = connection.query(sql, as: :array, cache_rows: false)
          result.each do |row|
            block.call(row)
          end
        end
      end

      private

      def set_default_ssl_mode_if_needed
        return unless config[:ssl] && !extra_connection_params[:ssl_mode]

        extra_connection_params[:sslmode] = 'required'
      end

      def valid_config?
        super
        require 'mysql2'
      rescue LoadError
        raise ConfigError, "Required 'MySql2' gem missing. Please add it to your Gemfile."
      end

      def result_to_csv(result)
        CSV.generate do |csv|
          csv << result.fields
          result.each do |row|
            csv << row
          end
        end
      end
    end
  end
end
