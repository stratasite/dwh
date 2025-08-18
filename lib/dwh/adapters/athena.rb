require 'csv'
require 'aws-sdk-athena'
require 'aws-sdk-s3'

module DWH
  module Adapters
    # AWS Athena adapter. Please ensure the aws-sdk-athena and aws-sdk-s3 gems are available before using this adapter.
    # Generally, adapters should be created using {DWH::Factory#create DWH.create}. Where a configuration
    # is passed in as options hash or argument list.
    #
    # @example Basic connection with required options
    #   DWH.create(:athena, {
    #     region: 'us-east-1',
    #     database: 'default',
    #     s3_output_location: 's3://my-athena-results-bucket/queries/',
    #     access_key_id: 'AKIAIOSFODNN7EXAMPLE',
    #     secret_access_key: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    #   })
    #
    # @example Connection with IAM role (recommended)
    #   DWH.create(:athena, {
    #     region: 'us-east-1',
    #     database: 'default',
    #     s3_output_location: 's3://my-athena-results-bucket/queries/'
    #   })
    #
    # @example Connection with workgroup
    #   DWH.create(:athena, {
    #     region: 'us-east-1',
    #     database: 'default',
    #     s3_output_location: 's3://my-athena-results-bucket/queries/',
    #     workgroup: 'my-workgroup'
    #   })
    class Athena < Adapter
      config :region, String, required: true, message: 'AWS region (e.g., us-east-1)'
      config :catalog, String, required: true, message: 'defaults to awsdatacatalog', default: 'awsdatacatalog'
      config :database, String, required: true, message: 'Athena database/schema name. defaults to default.', default: 'default'
      config :s3_output_location, String, required: true, message: 'S3 location for query results (e.g., s3://bucket/path/)'
      config :access_key_id, String, required: false, default: nil, message: 'AWS access key ID (optional if using IAM role)'
      config :secret_access_key, String, required: false, default: nil, message: 'AWS secret access key (optional if using IAM role)'
      config :workgroup, String, required: false, default: 'primary', message: 'Athena workgroup name'
      config :query_timeout, Integer, required: false, default: 300, message: 'query execution timeout in seconds'
      config :poll_interval, Integer, required: false, default: 2, message: 'polling interval in seconds for query status'
      config :client_name, String, required: false, default: 'DWH Ruby Gem', message: 'The name of the connecting app'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        aws_config = {
          region: config[:region]
        }

        # Add credentials if provided, otherwise rely on IAM role or environment
        if config[:access_key_id] && config[:secret_access_key]
          aws_config[:credentials] = Aws::Credentials.new(
            config[:access_key_id],
            config[:secret_access_key]
          )
        end

        # Merge any extra connection params
        aws_config.merge!(extra_connection_params)

        @connection = Aws::Athena::Client.new(aws_config)
        @s3_output_location = Aws::S3::Client.new(aws_config)

        @connection
      rescue StandardError => e
        raise ConfigError, "Failed to connect to Athena: #{e.message}"
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        # Test connection by listing workgroups
        connection.list_work_groups(max_results: 1)
        true
      rescue StandardError => e
        raise ConnectionError, "Athena connection test failed: #{e.message}" if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables(**qualifiers)
        schema = qualifiers[:database] || qualifiers[:schema] || config[:database]
        catalog = qualifiers[:catalog] || config[:catalog]

        sql = 'SELECT table_name FROM information_schema.tables'
        wheres = ['WHERE 1=1']
        wheres << "table_catalog = '#{catalog}'"
        wheres << "table_schema = '#{schema}'"

        result = execute("#{sql} #{wheres.join(' AND ')}", format: :array)
        result.flatten
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        database_name = qualifiers[:database] || config[:database]
        full_table_name = "#{database_name}.#{table}"

        sql_parts = ['SELECT COUNT(*) as row_count']

        if date_column
          sql_parts << ", MIN(#{date_column}) as date_start"
          sql_parts << ", MAX(#{date_column}) as date_end"
        end

        sql = "#{sql_parts.join} FROM #{full_table_name}"

        result = execute(sql, format: :object)
        first_row = result.first || {}

        TableStats.new(
          row_count: first_row['row_count'],
          date_start: first_row['date_start'],
          date_end: first_row['date_end']
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        schema = qualifiers[:database] || qualifiers[:schema] || config[:database]
        catalog =  qualifiers[:catalog] || config[:catalog]
        db_table = Table.new table, schema: schema, catalog: catalog

        sql = 'SELECT * FROM information_schema.columns'
        wheres = ["WHERE table_name = '#{db_table.physical_name}'"]

        wheres << "table_schema = '#{db_table.schema}'" if db_table.schema
        wheres << "table_catalog = '#{db_table.catalog}'" if db_table.catalog

        cols = execute("#{sql} \n #{wheres.join(' AND ')}", format: :object)
        cols.each do |col|
          # Athena DESCRIBE returns different column names than standard information_schema
          column_name = col['col_name'] || col['column_name']
          data_type = col['data_type']

          # Parse Athena data types (e.g., "varchar(255)", "decimal(10,2)")
          precision, scale = parse_data_type_precision(data_type)
          max_char_length = parse_char_length(data_type)

          db_table << Column.new(
            name: column_name,
            data_type: data_type,
            precision: precision,
            scale: scale,
            max_char_length: max_char_length
          )
        end

        db_table
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        begin
          result_data = with_debug(sql) { with_retry(retries) { execute_query(sql) } }
        rescue ExecutionError
          raise
        rescue StandardError => e
          raise ExecutionError, "Athena query failed: #{e.message}"
        end

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array
          result_data[:rows]
        when :object
          headers = result_data[:headers]
          result_data[:rows].map { |row| Hash[headers.zip(row)] }
        when :csv
          rows_to_csv(result_data[:headers], result_data[:rows])
        when :native
          result_data
        else
          raise UnsupportedCapability, "Unsupported format: #{format} for Athena adapter"
        end
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 0)
        with_debug(sql) do
          with_retry(retries) do
            execute_query(sql, io: io, stats: stats)
          end
        end
      rescue StandardError => e
        raise ExecutionError, "Athena streaming query failed: #{e.message}"
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        with_debug(sql) do
          result_data = execute_query(sql)

          result_data[:rows].each do |row|
            block.call(row)
          end
        end
      end

      def valid_config?
        super
        require 'aws-sdk-athena'
        require 'aws-sdk-s3'
      rescue LoadError
        raise ConfigError, "Required 'aws-sdk-athena' and 'aws-sdk-s3' gems missing. Please add them to your Gemfile."
      end

      private

      # Execute a query and return the parsed results
      def execute_query(sql, io: nil, stats: nil)
        query_execution_id = start_query_execution(sql)
        wait_for_query_completion(query_execution_id)
        if io
          fetch_query_results_to_io(query_execution_id, io, stats: stats)
        else
          fetch_query_results(query_execution_id)
        end
      end

      # Start query execution and return execution ID
      def start_query_execution(sql)
        params = {
          query_string: sql,
          query_execution_context: {
            catalog: config[:catalog],
            database: config[:database]
          },
          result_configuration: {
            output_location: config[:s3_output_location]
          },
          work_group: config[:workgroup]
        }

        response = connection.start_query_execution(params)
        response.query_execution_id
      end

      # Wait for query to complete
      def wait_for_query_completion(query_execution_id)
        timeout = config[:query_timeout]
        start_time = Time.now

        loop do
          raise ExecutionError, "Query timeout after #{timeout} seconds" if Time.now - start_time > timeout

          response = connection.get_query_execution(
            query_execution_id: query_execution_id
          )

          state = response.query_execution.status.state

          case state
          when 'SUCCEEDED'
            return true
          when 'FAILED', 'CANCELLED'
            reason = response.query_execution.status.state_change_reason
            raise ExecutionError, "Query #{state.downcase}: #{reason}"
          when 'QUEUED', 'RUNNING'
            sleep(config[:poll_interval])
            next
          else
            raise ExecutionError, "Unknown query state: #{state}"
          end
        end
      end

      # Fetch and parse query results
      def fetch_query_results(query_execution_id)
        headers = []
        rows = []
        next_token = nil

        loop do
          params = { query_execution_id: query_execution_id }
          params[:next_token] = next_token if next_token

          response = connection.get_query_results(params)
          headers = response.result_set.result_set_metadata.column_info.map(&:name) if headers.empty? && response.result_set.result_set_metadata

          response.result_set.rows.each_with_index do |row, index|
            # skip headers. first row on the first page is headers
            # we only skip first row on the first page with headers
            next if headers.empty? || (next_token.nil? && index.zero?)

            row_data = row.data.map { |datum| datum.var_char_value }
            rows << row_data unless row_data.compact.empty? # skip empty rows
          end

          next_token = response.next_token
          break unless next_token
        end

        { headers: headers, rows: rows }
      end

      # Fetch and parse query results
      def fetch_query_results_to_io(query_execution_id, io, stats: nil)
        headers = []
        next_token = nil
        wrote_headers = false

        loop do
          params = { query_execution_id: query_execution_id }
          params[:next_token] = next_token if next_token

          response = connection.get_query_results(params)

          if headers.empty? && response.result_set.result_set_metadata
            headers = response.result_set.result_set_metadata.column_info.map(&:name)
            io.write(CSV.generate_line(headers)) unless wrote_headers || headers.empty?
            wrote_headers = headers.empty?
          end

          response.result_set.rows.each_with_index do |row, index|
            next if headers.empty? || (next_token.nil? && index.zero?)

            row_data = row.data.map { |datum| datum.var_char_value }
            stats << row_data unless stats.nil?
            io.write(CSV.generate_line(row_data))
          end

          next_token = response.next_token
          break unless next_token
        end

        io.rewind
        io
      end

      # Parse precision and scale from data type string
      def parse_data_type_precision(data_type)
        if data_type && (match = data_type.match(/\((\d+)(?:,\s*(\d+))?\)/))
          precision = match[1].to_i
          scale = match[2]&.to_i
          [precision, scale]
        else
          [nil, nil]
        end
      end

      # Parse character length from data type string
      def parse_char_length(data_type)
        if data_type && (match = data_type.match(/(?:var)?char\((\d+)\)/i))
          match[1].to_i
        end
      end

      # Convert headers and rows to CSV string
      def rows_to_csv(headers, rows)
        CSV.generate do |csv|
          csv << headers
          rows.each { |row| csv << row }
        end
      end
    end
  end
end
