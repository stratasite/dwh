require 'jwt'
require 'csv'
require 'base64'
require 'digest'

module DWH
  module Adapters
    # Snowflake adapter for executing SQL queries against Snowflake databases.
    #
    # Supports two authentication modes:
    # - Personal Access Token (pat)
    # - Key Pair Authentication (kp)
    #
    # @example Basic connection with Personal Access Token
    #   DWH.create(:snowflake, {
    #     auth_mode: 'pat',
    #     account_identifier: 'myorg-myaccount',
    #     personal_access_token: 'your-token-here',
    #     warehouse: 'COMPUTE_WH',
    #     database: 'ANALYTICS',
    #     schema: 'PUBLIC'
    #   })
    #
    # @example Connection with Key Pair Authentication
    #   DWH.create(:snowflake, {
    #     auth_mode: 'kp',
    #     account_identifier: 'myorg-myaccount.us-east-1',
    #     username: 'john_doe',
    #     private_key: '/path/to/private_key.pem',
    #     warehouse: 'COMPUTE_WH',
    #     database: 'ANALYTICS'
    #   })
    class Snowflake < Adapter
      # Authentication configuration
      config :auth_mode, String,
             required: true, allowed: %w[pat kp],
             message: 'Authentication mode: "pat" (Personal Access Token) or "kp" (Key Pair)'

      config :account_identifier, String,
             required: true, message: 'Snowflake account identifier (e.g., myorg-myaccount or myorg-myaccount.region)'

      # Personal Access Token authentication
      config :personal_access_token, String,
             required: false, message: 'Personal access token (required when auth_mode is "pat")'

      # Key Pair authentication
      config :username, String,
             required: false, message: 'Username (required when auth_mode is "kp")'

      config :private_key, String,
             required: false, message: 'Private key file path or private key content (required when auth_mode is "kp")'

      config :public_key_fp, String,
             required: false, message: 'Public key fingerprint (optional, will be derived if not provided)'

      # Connection configuration
      config :client_name, String,
             required: false,
             default: 'Ruby DWH Gem', message: 'Client name sent to Snowflake'

      config :query_timeout, Integer,
             required: false,
             default: 3600, message: 'Query execution timeout in seconds'

      # Database configuration
      config :role, String,
             required: false, message: 'Snowflake role to assume'

      config :warehouse, String,
             required: false, message: 'Snowflake warehouse to use'

      config :database, String,
             required: true, message: 'Specific database to connect to.'

      config :schema, String,
             required: false, message: 'Default schema'

      # Constants
      AUTH_TOKEN_TYPES = {
        pat: 'PROGRAMMATIC_ACCESS_TOKEN',
        kp: 'KEYPAIR_JWT'
      }.freeze

      API_ENDPOINTS = {
        statements: '/api/v2/statements'
      }.freeze

      DEFAULT_PARAMETERS = {
        DATE_OUTPUT_FORMAT: 'YYYY-MM-DD',
        TIMESTAMP_OUTPUT_FORMAT: 'YYYY-MM-DD HH24:MI:SS',
        TIMESTAMP_TZ_OUTPUT_FORMAT: 'YYYY-MM-DD HH24:MI:SS TZH',
        TIMESTAMP_NTZ_OUTPUT_FORMAT: 'YYYY-MM-DD HH24:MI:SS',
        TIMESTAMP_LTZ_OUTPUT_FORMAT: 'YYYY-MM-DD HH24:MI:SS TZH',
        TIME_OUTPUT_FORMAT: 'HH24:MI:SS'
      }.freeze

      DEFAULT_POLL_INTERVAL = 0.25
      MAX_POLL_INTERVAL = 30
      TOKEN_VALIDITY_HOURS = 1.0

      def initialize(config)
        super
        validate_auth_config
      end

      # (see Adapter#connection)
      def connection
        return @connection if @connection && !token_expired?

        reset_connection if token_expired?
        @token_expires_at = Time.now + TOKEN_VALIDITY_HOURS / 24.0

        @connection = Faraday.new(
          url: "https://#{config[:account_identifier]}.snowflakecomputing.com",
          headers: {
            'Content-Type' => 'application/json',
            'Authorization' => "Bearer #{auth_token}",
            'X-Snowflake-Authorization-Token-Type' => auth_token_type,
            'User-Agent' => config[:client_name]
          },
          request: {
            timeout: config[:query_timeout]
          }.merge(extra_connection_params)
        )
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        execute('SELECT 1')
        true
      rescue StandardError => e
        raise ConnectionError, "Failed to connect to Snowflake: #{e.message}" if raise_exception

        logger.error "Connection test failed: #{e.message}"
        false
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        result = with_retry(retries + 1) do
          with_debug(sql) do
            response = submit_query(sql)
            fetch_data(handle_query_response(response))
          end
        end

        format_result(result, format)
      end

      # (see Adapter#execute)
      def execute_stream(sql, io, stats: nil, retries: 0)
        with_retry(retries) do
          with_debug(sql) do
            response = submit_query(sql)
            fetch_data(handle_query_response(response), io: io, stats: stats)
          end
        end

        io.rewind
        io
      end

      # Execute SQL query and yield streamed results
      # @param sql [String] SQL query to execute
      # @yield [chunk] yields each chunk of data as it's processed
      def stream(sql, &block)
        with_debug(sql) do
          response = submit_query(sql)
          fetch_data(handle_query_response(response), proc: block)
        end
      end

      # (see Adapter#tables)
      # @param database [String] optional database filter
      # @return [Array<String>] list of table names
      def tables(catalog: nil, schema: nil, database: nil)
        db = database || config[:database]
        sql = "SELECT table_name FROM #{db}.information_schema.tables"
        conditions = []

        conditions << "table_schema = '#{schema.upcase}'" if schema
        conditions << "table_catalog = '#{catalog.upcase}'" if catalog

        sql += " WHERE #{conditions.join(' AND ')}" if conditions.any?

        result = execute(sql)
        result.flatten
      end

      # (see Adapter#tables)
      # @param database [String] optional database
      # @return [DWH::Table] table metadata object
      def metadata(table, catalog: nil, schema: nil, database: nil)
        db_table = Table.new(table, schema: schema, catalog: catalog)
        db = database || config[:database]
        sql = <<~SQL
          SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length
          FROM #{db}.information_schema.columns
        SQL

        conditions = ["table_name = '#{db_table.physical_name.upcase}'"]
        conditions << "table_schema = '#{db_table.schema.upcase}'" if db_table.schema
        conditions << "table_catalog = '#{db_table.catalog.upcase}'" if db_table.catalog

        columns = execute("#{sql} WHERE #{conditions.join(' AND ')}")

        columns.each do |col|
          db_table << Column.new(
            name: col[0]&.downcase,
            data_type: col[1]&.downcase,
            precision: col[2],
            scale: col[3],
            max_char_length: col[4]
          )
        end

        db_table
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, catalog: nil, schema: nil, database: nil)
        date_fields = if date_column
                        ", MIN(#{date_column}) AS date_start, MAX(#{date_column}) AS date_end"
                      else
                        ', NULL AS date_start, NULL AS date_end'
                      end

        data = execute("SELECT COUNT(*) AS row_count#{date_fields} FROM #{table}")
        cols = data.first

        TableStats.new(
          row_count: cols[0],
          date_start: cols[1],
          date_end: cols[2]
        )
      end

      private

      # Validation and Setup Methods
      def validate_auth_config
        case config[:auth_mode]
        when 'pat'
          return if config[:personal_access_token]

          raise ConfigError, "personal_access_token is required when auth_mode is 'pat'"
        when 'kp'
          raise ConfigError, "username is required when auth_mode is 'kp'" unless config[:username]
          return if config[:private_key]

          raise ConfigError, "private_key is required when auth_mode is 'kp'"
        else
          raise ConfigError, "Invalid auth_mode: #{config[:auth_mode]}"
        end
      end

      def token_expired?
        @token_expires_at.nil? || Time.now >= @token_expires_at
      end

      def reset_connection
        @token_expires_at = nil
        @jwt_token = nil
        close
      end

      # Authentication
      def auth_token
        personal_access_token_mode? ? config[:personal_access_token] : jwt_token
      end

      def auth_token_type
        AUTH_TOKEN_TYPES[config[:auth_mode].to_sym]
      end

      def personal_access_token_mode?
        config[:auth_mode] == 'pat'
      end

      def key_pair_mode?
        config[:auth_mode] == 'kp'
      end

      def jwt_token
        @jwt_token ||= JWT.encode(
          {
            iss: "#{qualified_username}.SHA256:#{public_key_fingerprint}",
            sub: qualified_username,
            iat: Time.now.to_i,
            exp: @token_expires_at.to_i
          },
          private_key_object, 'RS256'
        )
      end

      def qualified_username
        "#{account_identifier.upcase}.#{config[:username].upcase}"
      end

      def private_key_object
        @private_key_object ||= OpenSSL::PKey.read(
          if File.exist?(config[:private_key])
            File.read(config[:private_key])
          else
            config[:private_key]
          end
        )
      end

      def public_key_fingerprint
        @public_key_fingerprint ||=
          config[:public_key_fp] || Base64.strict_encode64(
            Digest::SHA256.digest(private_key_object.public_key.to_der)
          )
      end

      def submit_query(sql)
        connection.post(API_ENDPOINTS[:statements]) do |req|
          req.body =
            {
              statement: sql,
              timeout: config[:query_timeout],
              warehouse: config[:warehouse]&.upcase,
              database: config[:database]&.upcase,
              schema: config[:schema]&.upcase,
              role: config[:role]&.upcase,
              parameters: DEFAULT_PARAMETERS
            }.compact.merge(extra_query_params)
            .to_json
        end
      end

      def handle_query_response(response)
        case response.status
        when 200
          JSON.parse(response.body)
        when 202
          poll(JSON.parse(response.body))
        else
          error_info = begin
            JSON.parse(response.body)
          rescue StandardError
            response.body
          end
          message = error_info.is_a?(Hash) ? error_info['message'] : error_info
          raise ExecutionError, "Snowflake query failed: #{message}"
        end
      end

      def poll(initial_result)
        statement_handle = initial_result['statementHandle']
        sleep_interval = DEFAULT_POLL_INTERVAL

        logger.debug "Polling for query completion: #{statement_handle}"

        loop do
          response = connection.get("#{API_ENDPOINTS[:statements]}/#{statement_handle}")
          result = JSON.parse(response.body)

          case response.status
          when 200
            return result
          when 202
            logger.debug "Query still running. Sleeping #{sleep_interval}s..."
            sleep(sleep_interval)
            # once we hit one max interval lets restart
            # the cycle.
            sleep_interval = sleep_interval == MAX_POLL_INTERVAL ? DEFAULT_POLL_INTERVAL : sleep_interval
            sleep_interval = [sleep_interval * 2, MAX_POLL_INTERVAL].min
          else
            message = result['message'] || result
            raise ExecutionError, "Polling failed: #{message}"
          end
        end
      end

      # Result Processing
      def format_result(result, format)
        data = result[:data]
        columns = result[:columns]

        case format
        when :array
          data
        when :object
          data.map { |row| columns.zip(row).to_h }
        when :csv
          CSV.generate do |csv|
            csv << columns
            data.each { |row| csv << row }
          end
        when :native
          result
        else
          raise UnsupportedCapability, "Unknown result format: #{format}"
        end
      end

      def fetch_data(result, io: nil, stats: nil, proc: nil)
        collector = {
          columns: result.dig('resultSetMetaData', 'rowType')&.map { |col| col['name'] } || [],
          data: [], io: io, stats: stats, wrote_header: false
        }

        partitions = result.dig('resultSetMetaData', 'partitionInfo')
        write_data(result['data'], collector, io, stats, proc)
        return collector unless partitions.size > 1

        url = "#{API_ENDPOINTS[:statements]}/#{result['statementHandle']}?partition="
        partitions[1..].each.with_index(1) do |_, index|
          logger.debug "Fetching partition #{index} of #{partitions.length - 1} for statement handle: #{result['statementHandle']}"
          resp = connection.get(url + index.to_s)
          raise ExecutionError, "Could not data partitions from Snowflake: #{resp.body}" unless resp.status == 200

          part_res = JSON.parse(resp.body)

          write_data(part_res['data'], collector, io, stats, proc)
        end

        collector
      end

      def write_data(data, collector, io = nil, stats = nil, proc = nil)
        if io
          unless collector[:wrote_header]
            io << CSV.generate_line(collector[:columns])
            collector[:wrote_header] = true
          end

          data.each do |row|
            stats << row if stats
            io << CSV.generate_line(row)
          end
        elsif proc
          data.each { proc.call(it) }
        else
          data.each { collector[:data] << it }
        end

        collector
      end
    end
  end
end
