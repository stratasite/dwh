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
    #   DWH.create(:duckdb, {file: 'path/to/my/duckdb' })
    #
    # @example Open in read only mode. {https://duckdb.org/docs/stable/configuration/overview#configuration-reference config docs}
    #   DWH.create(:duckdb, {file: 'path/to/my/duckdb' ,duck_config: { access_mode: "READ_ONLY"}})
    class DuckDb < Adapter
      config :file, String, required: true, message: 'path/to/duckdb/db'
      config :schema, String, required: false, default: 'main', message: 'schema defaults to main'
      config :duck_config, Hash, required: false, message: 'hash of valid DuckDb configuration options'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        if self.class.databases.key?(config[:file])
          @db = self.class.databases[config[:file]]
        else
          ducked_config = DuckDB::Config.new
          if config.key?(:duck_config)
            config[:duck_config].each do |key, val|
              ducked_config[key.to_s] = val
            end
          end
          @db = DuckDB::Database.open(config[:file], ducked_config)
          self.class.databases[config[:file]] = @db
        end

        @connection = @db.connect

        @connection
      rescue StandardError => e
        raise ConfigError, e.message
      end

      def self.databases
        @databases ||= {}
      end

      def self.open_databases
        databases.size
      end

      # DuckDB is an in process database so we don't want to
      # open multiple instances of the same db in memory. Rather,
      # we open one instance but many connections. Use this
      # method to close them all.
      def self.close_all
        databases.each do |key, db|
          db.close
          databases.delete(key)
        end
      end

      # This disconnects the current connection but
      # the db is still in process and can be reconnected
      # to.
      #
      # (see Adapter#close)
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
        catalog, schema = qualifiers.values_at(:catalog, :schema)
        sql = 'SELECT table_name FROM duckdb_tables'

        where = []
        where << "database_name = '#{catalog}'" if catalog

        where << if schema
                   "schema_name = '#{schema}'"
                 else
                   "schema_name = '#{config[:schema]}'"
                 end

        res = execute("#{sql} WHERE #{where.join(' AND ')}")
        res.flatten
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        qualifiers[:schema] = config[:schema] unless qualifiers[:schema]
        db_table = Table.new table, **qualifiers

        sql = <<-SQL
        SELECT count(*) ROW_COUNT
        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
        FROM #{db_table.fully_qualified_table_name}
        SQL

        result = execute(sql)
        TableStats.new(
          row_count: result.first[0],
          date_start: result.first[1],
          date_end: result.first[2]
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, **qualifiers
        sql = 'SELECT column_name, data_type, character_maximum_length, numeric_precision,numeric_scale FROM duckdb_columns'

        where = ["table_name = '#{db_table.physical_name}'"]
        where << "database_name = '#{db_table.catalog}'" if db_table.catalog

        where << if db_table.schema
                   "schema_name = '#{db_table.schema}'"
                 else
                   "schema_name = '#{config[:schema]}'"
                 end

        cols = execute("#{sql} WHERE #{where.join(' AND ')}")
        cols.each do |col|
          db_table << Column.new(
            name: col[0],
            data_type: col[1],
            precision: col[3],
            scale: col[4],
            max_char_length: col[2]
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
          result = with_debug(sql) { with_retry(retries) { connection.query(sql) } }
        rescue StandardError => e
          raise ExecutionError, e.message
        end

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array
          result.to_a
        when :object
          result_to_hash(result)
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
            result = connection.query(sql)
            result.each do |row|
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
          result = connection.query(sql)
          result.each do |row|
            block.call(row)
          end
        end
      end

      def valid_config?
        super
        require 'duckdb'
      rescue LoadError
        raise ConfigError, "Required 'duckdb' gem missing. Please add it to your Gemfile."
      end

      private

      def result_to_hash(result)
        columns = result.columns.map(&:name)

        result.each.map do |row|
          columns.zip(row).to_h
        end
      end

      def result_to_csv(result)
        CSV.generate do |csv|
          csv << result.columns.map(&:name)
          result.each do |row|
            csv << row
          end
        end
      end
    end
  end
end
