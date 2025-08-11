module DWH
  module Adapters
    # Microsoft SQL Server adapter.  This adapter requires the {https://github.com/rails-sqlserver/tiny_tds tiny_tds}
    # gem. For Mac OS and Linux, you will need to instal FreeTDS and most likely needs OpenSSL. Please follow the
    # the instructions there before using the adapter.
    #
    # Create and adatper instance using {DWH::Factory#create DWH.create}.
    #
    # @example Basic connection with required only options
    #   DWH.create(:sqlserver, {host: 'localhost', database: 'my_db', username: 'sa'})
    #
    # @example Connect to Azuer SQL Server
    #   DWH.create(:sqlserver, {host: 'localhost', database: 'my_db', username: 'sa', azure: true})
    # @example Connection sending custom application name
    #   DWH.create(:sqlserver, {host: 'localhost', database: 'my_db', username: 'sa', client_name: 'Strata CLI'})
    #
    # @example Pass extra connection params
    #   DWH.create(:sqlserver, {host: 'localhost', database: 'my_db',
    #   username: 'sa', client_name: 'Strata CLI',
    #   extra_connection_params: {
    #     container: true,
    #     use_utf16: false
    #   })
    #
    # @example fetch tables in database
    #   adapter.tables
    #
    # @example fetch tables from another database
    #   adapter.tables(catalog: 'other_db')
    #
    # @example get table metadata for table in another db
    #   adapter.metadata('other_db.dbo.my_table') or adapter.metadata('my_table', catalog: 'other_db')
    class SqlServer < Adapter
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: false, default: 1433, message: 'port to connect to'
      config :database, String, required: true, message: 'name of database to connect to'
      config :username, String, required: true, message: 'connection username'
      config :password, String, required: false, default: nil, message: 'connection password'
      config :query_timeout, String, required: false, default: 3600, message: 'query execution timeout in seconds'
      config :client_name, String, required: false, default: 'DWH Ruby Gem', message: 'The name of the connecting app'
      config :azure, Boolean, required: false, default: false, message: 'signal whether this is an azure connection'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        properties = {
          host: config[:host],
          port: config[:port],
          database: config[:database],
          username: config[:username],
          password: config[:password],
          appname: config[:client_name],
          timeout: config[:query_timeout],
          azure: config[:azure]
        }.merge(extra_connection_params)

        @connection = TinyTds::Client.new(**properties)

        @connection
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
        change_current_database(qualifiers[:catalog])
        table_catalog = qualifiers[:catalog] || config[:database]
        table_schema_where = qualifiers[:schema] ? " AND table_schema = '#{qualifiers[:schema]}'" : ''

        sql = <<~SQL
          SELECT table_name
          FROM information_schema.tables
          WHERE table_catalog = '#{table_catalog}'
          #{table_schema_where}
        SQL

        execute(sql)
      ensure
        reset_current_database
      end

      # Changes the default database to the one specified here.
      # will store the default db in @current_database attr.
      # @param catalog [String] new database to use
      def change_current_database(catalog = nil)
        return unless catalog && catalog != config[:database]

        @current_database = catalog
        use(catalog)
      end

      # Resets the default database to the configured one
      # if it was changed
      def reset_current_database
        return if @current_database.nil? || @current_database == config[:database]

        use(config[:database])
      end

      # (see Adapter#table?)
      def table?(table_name)
        tables.include?(table_name)
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        change_current_database(qualifiers[:catalog])
        table_name = qualifiers[:schema] ? "#{qualifiers[:schema]}.#{table}" : table
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM #{quote(table_name)}
        SQL

        result = connection.execute(sql)
        row = result.to_a(empty_sets: true).first
        TableStats.new(
          row_count: row['ROW_COUNT'],
          date_start: row['DATE_START'],
          date_end: row['DATE_END']
        )
      ensure
        reset_current_database
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, **qualifiers
        change_current_database(db_table.catalog)

        schema_where = ''
        schema_where = "AND table_schema = '#{db_table.schema}'" if db_table.schema.present?

        sql = <<-SQL
                    SELECT column_name, data_type, character_maximum_length, numeric_precision,numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = '#{db_table.physical_name}'
                    #{schema_where}
        SQL
        cols = execute(sql, format: 'object')
        cols.each do |col|
          db_table << Column.new(
            name: col['column_name'],
            data_type: col['data_type'],
            precision: col['numeric_precision'],
            scale: col['numeric_scale'],
            max_char_length: col['character_maximum_length']
          )
        end

        db_table
      ensure
        reset_current_database
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        result = with_debug(sql) { with_retry(retries) { connection.execute(sql) } }

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array
          result.to_a(as: :array, empty_sets: true, timezone: :utc)
        when :object
          result.to_a(as: :hash, empty_sets: true, timezone: :utc)
        when :csv
          result_to_csv(result)
        when :native
          result
        else
          raise UnsupportedCapability, "Unsupported format: #{format} for this #{name}"
        end
      rescue TinyTds::Error => e
        raise ExecutionError, e.message
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 0)
        with_debug(sql) do
          with_retry(retries) do
            result = connection.execute(sql)
            result.each(as: :array, empty_sets: true, cache_rows: false, timezone: :utc) do |row|
              stats << row unless stats.nil?
              io.write(CSV.generate_line(row))
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
          result = connection.execute(sql)
          result.each(empty_sets: true, cache_rows: false, timezone: :utc) do |row|
            block.call(row)
          end
        end
      end

      def extract_day_name(exp, abbreviate: false)
        exp = cast(exp, 'date') unless exp =~ /cast/i
        super(exp, abbreviate: abbreviate).downcase
      end

      def extract_month_name(exp, abbreviate: false)
        exp = cast(exp, 'date') unless exp =~ /cast/i
        if abbreviate
          "UPPER(LEFT(DATENAME(month, #{exp}), 3))"
        else
          "UPPER(DATENAME(month, #{exp}))"
        end
      end

      def valid_config?
        super
        require 'tiny_tds'
      rescue LoadError
        raise ConfigError, "Required 'tiny_tds' gem missing. Please add it to your Gemfile."
      end

      private

      def use(new_database)
        res = connection.execute("use #{quote(new_database)}")
        res.do
      end

      def result_to_csv(result)
        CSV.generate do |csv|
          csv << result.fields
          result.each(as: :array, empty_sets: true) do |row|
            csv << row
          end
        end
      end
    end
  end
end
