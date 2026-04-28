require 'csv'
require_relative 'open_authorizable'

module DWH
  module Adapters
    # Databricks adapter for executing SQL queries against Databricks SQL warehouses.
    #
    # Supports OAuth M2M (service principal) and U2M (authorization code) flows.
    # The host application must set auth_mode explicitly:
    # - oauth_m2m: client_credentials flow
    # - oauth_u2m: authorization_code + PKCE flow
    #
    # @example Connection with OAuth (service principal)
    #   DWH.create(:databricks, {
    #     host: 'adb-1234567890123456.7.azuredatabricks.net',
    #     warehouse: 'abc123def456',
    #     oauth_client_id: 'service-principal-app-id',
    #     oauth_client_secret: 'your-oauth-secret-here',
    #     catalog: 'main',
    #     schema: 'default'
    #   })
    class Databricks < Adapter
      include OpenAuthorizable

      oauth_with authorize: ->(adapter) { "https://#{adapter.host}/oidc/v1/authorize" },
                 tokenize: ->(adapter) { "https://#{adapter.host}/oidc/v1/token" },
                 default_scope: 'all-apis'

      config :host, String, required: true, message: 'Databricks workspace host (e.g., adb-xxx.databricks.cloud.com)'
      config :auth_mode, String, required: true, allowed: %w[oauth_m2m oauth_u2m],
                         message: 'Authentication mode: oauth_m2m or oauth_u2m'
      config :oauth_client_id, String, required: true, message: 'OAuth client ID (service principal application ID)'
      config :oauth_client_secret, String, required: true, message: 'OAuth client secret'
      config :client_name, String, required: false, default: 'Ruby DWH Gem', message: 'Client name sent to Databricks'
      config :query_timeout, Integer, required: false, default: 3600, message: 'Query execution timeout in seconds'
      config :warehouse, String, required: true, message: 'Databricks SQL warehouse ID to use for query execution'
      config :catalog, String, required: false, message: 'Default catalog (Unity Catalog)'
      config :schema, String, required: false, message: 'Default schema'

      DEFAULT_POLL_INTERVAL = 0.25
      MAX_POLL_INTERVAL = 30

      STATEMENTS_API = '/api/2.0/sql/statements'.freeze

      def initialize(config)
        super
        validate_oauth_config
      end

      def connection
        return @connection if @connection && !token_expired?

        reset_connection if token_expired?
        @connection = Faraday.new(
          url: "https://#{workspace_host}",
          headers: {
            'Content-Type' => 'application/json',
            'Authorization' => "Bearer #{oauth_access_token}",
            'User-Agent' => config[:client_name]
          },
          request: {
            timeout: config[:query_timeout]
          }.merge(extra_connection_params)
        )
      end

      def test_connection(raise_exception: false)
        execute('SELECT 1')
        true
      rescue StandardError => e
        raise ConnectionError, "Failed to connect to Databricks: #{e.message}" if raise_exception

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

      def tables(**qualifiers)
        catalog = qualifiers[:catalog] || config[:catalog]
        schema = qualifiers[:schema] || config[:schema]

        raise ConfigError, 'catalog is required for Databricks tables query' unless catalog

        sql = "SELECT table_name FROM #{catalog}.information_schema.tables"
        sql += " WHERE table_schema = '#{schema}'" if schema

        result = execute(sql)
        result.flatten
      end

      def metadata(table, **qualifiers)
        catalog = qualifiers[:catalog] || config[:catalog]
        schema = qualifiers[:schema] || config[:schema]

        raise ConfigError, 'catalog is required for Databricks metadata query' unless catalog

        db_table = Table.new(table, schema: schema, catalog: catalog)

        sql = <<~SQL
          SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length
          FROM #{catalog}.information_schema.columns
          WHERE table_name = '#{db_table.physical_name}'
        SQL
        sql += " AND table_schema = '#{db_table.schema}'" if db_table.schema

        columns = execute(sql)

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

      def stats(table, date_column: nil)
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

      def reset_connection
        @oauth_access_token = nil
        @oauth_refresh_token = nil
        @token_expires_at = nil
        close
      end

      def submit_query(sql)
        connection.post(STATEMENTS_API) do |req|
          req.body = {
            statement: sql,
            warehouse_id: config[:warehouse],
            catalog: config[:catalog],
            schema: config[:schema],
            wait_timeout: '30s',
            on_wait_timeout: 'CONTINUE',
            format: 'JSON_ARRAY',
            disposition: 'INLINE'
          }.compact.merge(extra_query_params).to_json
        end
      end

      def handle_query_response(response)
        body = JSON.parse(response.body)

        case response.status
        when 200
          state = body.dig('status', 'state')
          state == 'SUCCEEDED' ? body : poll(body['statement_id'])
        when 202
          poll(body['statement_id'])
        else
          error_message = body['message'] || body['error_code'] || response.body
          raise ExecutionError, "Databricks query failed (#{response.status}): #{error_message}"
        end
      end

      def poll(statement_id)
        sleep_interval = DEFAULT_POLL_INTERVAL

        logger.debug "Polling for query completion: #{statement_id}"

        loop do
          response = connection.get("#{STATEMENTS_API}/#{statement_id}")
          body = JSON.parse(response.body)
          state = body.dig('status', 'state')

          case state
          when 'SUCCEEDED'
            return body
          when 'FAILED', 'CANCELED', 'CLOSED'
            error_msg = body.dig('status', 'error', 'message') || state
            raise ExecutionError, "Databricks query #{state}: #{error_msg}"
          else
            logger.debug "Query still running (state: #{state}). Sleeping #{sleep_interval}s..."
            sleep(sleep_interval)
            sleep_interval = sleep_interval == MAX_POLL_INTERVAL ? DEFAULT_POLL_INTERVAL : sleep_interval
            sleep_interval = [sleep_interval * 2, MAX_POLL_INTERVAL].min
          end
        end
      end

      def fetch_data(result, io: nil, stats: nil, proc: nil)
        columns = result.dig('manifest', 'schema', 'columns')&.map { |col| col['name'] } || []
        chunks = result.dig('manifest', 'chunks') || []
        collector = {
          columns: columns,
          data: [],
          io: io,
          stats: stats,
          wrote_header: false
        }

        write_data(result.dig('result', 'data_array') || [], collector, io, stats, proc)

        return collector unless chunks.size > 1

        statement_id = result['statement_id']
        chunks[1..].each do |chunk|
          chunk_index = chunk['chunk_index']
          logger.debug "Fetching chunk #{chunk_index} of #{chunks.size} for statement: #{statement_id}"

          resp = connection.get("#{STATEMENTS_API}/#{statement_id}/result/chunks/#{chunk_index}")
          raise ExecutionError, "Failed to fetch chunk #{chunk_index}: #{resp.body}" unless resp.status == 200

          chunk_data = JSON.parse(resp.body)
          write_data(chunk_data['data_array'] || [], collector, io, stats, proc)
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

      def workspace_host
        config[:host].to_s
      end

      def oauth_supports_authorization_code_flow?
        auth_mode == 'oauth_u2m'
      end

      def oauth_supports_client_credentials_flow?
        auth_mode == 'oauth_m2m'
      end

      def oauth_redirect_uri_required?
        oauth_supports_authorization_code_flow?
      end

      def oauth_client_credentials_params
        {
          grant_type: 'client_credentials',
          scope: 'all-apis'
        }
      end

      def oauth_token_expiry_leeway_seconds
        30
      end

      def oauth_uses_pkce?
        oauth_supports_authorization_code_flow?
      end
    end
  end
end
