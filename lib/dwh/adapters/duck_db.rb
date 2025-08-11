module DWH
  module Adapters
    # DuckDb adapter.
    #
    # This requires the ruby {https://github.com/suketa/ruby-duckdb DuckDb} gem.  Installation
    # is a bit complex. Please follow the guide on the gems page to make sure
    # you have DuckDb installed as required before installing the gem.
    #
    # Generally, adapters should be created using {DWH::Factory#create DWH.create}. Where a configuration
    # is passed in as options hash or argument list.
    #
    # @example Basic connection with required only options
    #   DWH.create(:druid, {host: 'localhost',port: 8080, protocol: 'http'})
    #
    # @example Connect with SSL and basic authorization
    #   DWH.create(:druid, {host: 'localhost',port: 8080, protocol: 'http',
    #       basic_auth: 'BASE_64 encoded authorization key'
    #   })
    #
    # @example Sending custom client name and user information
    #   DWH.create(:druid, {host: 'localhost',port: 8080,
    #     client_name: 'Strata CLI', extra_connection_params: {
    #       context: {
    #         user: 'Ajo',
    #         team: 'Engineering'
    #       }
    #     }})
    class DuckDb < Adapter
      DATABASES = {}
      config :file, String, required: true, message: 'path/to/duckdb/db'
      config :duck_config, Hash, required: false, message: 'hash of valid DuckDb configuration options'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        if DATABASES.key?(config[:file])
          @db = DATABASES[config[:file]]
        else
          ducked_config = DuckDB::Config.new
          if config.key?(:duck_config)
            config[:duck_config].each do |key, val|
              ducked_config[key.to_s] = val
            end
          end
          @db = DuckDB::Database.open(config[:file], ducked_config)
          DATABASES[config[:file]] = @db
        end

        @connection = @db.connect

        @connection
      rescue StandardError => e
        raise ConfigError, e.message
      end

      def self.open_databases
        DATABASES.size
      end

      # DuckDB is an in process database so we don't want to
      # open multiple instances of the same db in memory. Rather,
      # we open one instance but many connections. Use this
      # method to close them all.
      def self.close_all
        DATABASES.each_value(&:close)
        DATABASES = {}
      end

      def close
        connection.disconnect
        @connection = nil
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
        sql = if schema? || qualifiers[:schema]
                <<-SQL
                        SELECT table_schema || '.' || table_name#{' '}
                        FROM information_schema.tables
                        WHERE table_schema in (#{qualified_schema_name(qualifiers)})
                SQL
              else
                <<-SQL
                        SELECT table_name#{' '}
                        FROM information_schema.tables
                SQL
              end

        result = connection.exec(sql)
        result.values.flatten
      end

      # (see Adapter#table?)
      def table?(table_name)
        tables.include?(table_name)
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        table_name = qualifiers[:schema] ? "#{qualifiers[:schema]}.#{table}" : table
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM "#{table_name}"
        SQL

        result = connection.exec(sql)
        TableStats.new(
          row_count: result.first['row_count'],
          date_start: result.first['date_start'],
          date_end: result.first['date_end']
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, schema: qualifiers[:schema]

        schema_where = ''
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
      end

      # True if the configuration was setup with a schema.
      def schema?
        config[:schema].present?
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        begin
          result = with_debug(sql) { with_retry(retries) { connection.exec(sql) } }
        rescue StandardError => e
          raise ExecutionError, e.message
        end

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array
          result.values
        when :object
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
            connection.exec(sql) do |result|
              result.each_row do |row|
                stats << row unless stats.nil?
                io.write(CSV.generate_line(row))
              end
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
          connection.exec(sql) do |result|
            result.each_row do |row|
              block.call(row)
            end
          end
        end
      end

      # Need to override default add method
      # since postgres doesn't support quarter as an
      # interval.
      # @param unit [String] Should be one of day, month, quarter etc
      # @param val [String, Integer] The number of days to add
      # @param exp [String] The sql expresssion to modify
      def date_add(unit, val, exp)
        if unit.downcase.strip == 'quarter'
          unit = 'months'
          val = val.to_i * 3
        end
        gsk(:date_add)
          .gsub('@UNIT', unit)
          .gsub('@VAL', val.to_s)
          .gsub('@EXP', exp)
      end

      def valid_config?
        super
        require 'duckdb'
      rescue LoadError
        raise ConfigError, "Required 'duckdb' gem missing. Please add it to your Gemfile."
      end

      private

      def set_default_ssl_mode_if_needed
        return unless config[:ssl] && !extra_connection_params[:sslmode]

        extra_connection_params[:sslmode] = 'require'
      end

      def qualified_schema_name(qualifiers = {})
        qs = qualifiers[:schema] || config[:schema]
        @qualified_schema_name ||= qs.split(',').map { |s| "'#{s}'" }.join(',')
      end

      def result_to_csv(result)
        CSV.generate do |csv|
          csv << result.fields
          result.each do |row|
            csv << row.values # default is hash
          end
        end
      end
    end
  end
end
