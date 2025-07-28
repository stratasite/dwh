require "jwt"

module DWH
  module Adapters
    class Snowflake < Adapter
      define_config :host, required: true, message: "server host ip address or domain name"
      define_config :account_identifier, required: false, message: "snowflake account identifier"
      define_config :username, required: true, message: "connection username"
      define_config :private_key, required: true, message: "private key file path or private key"
      define_config :public_key_fp, required: false,
        message: "optional public key finger print. will derive if omitted."
      define_config :query_timeout, required: false, default: 3600, message: "query execution timeout in seconds"
      define_config :role, required: false, default: nil, message: "role to connect with"
      define_config :warehouse, required: false, default: nil, message: "snowflake warehouse to connect to"
      define_config :database, required: false, default: nil, message: "default namespace or database to connect to"
      define_config :schema, required: false, default: nil, message: "schema to connect to"

      def connection
        return @connection if @connection.present? && !expired?

        if @connection.present? && expired?
          logger.debug "Resetting expired connection"
          reset_connection
        end

        @connection = Faraday.new(
          url: "https://#{config[:host].split("/").first}",
          headers: {
            "Content-Type" => "application/json",
            "Authorization" => "Bearer #{jwt_token}",
            "X-Snowflake-Authorization-Token-Type" => "KEYPAIR_JWT",
            "User-Agent" => "Ruby dwh-#{VERSION}"
          },
          request: {
            timeout: config[:query_timeout]
          }.merge(extra_connection_params)
        )
      end

      def close
        connection.close if @connection
        @connection = nil
      end

      def execute_stream(sql, io, memory_row_limit: 20_000, stats: nil)
        execute(sql, io, memory_row_limit: memory_row_limit, stats: stats)
      end

      SNOWFLAKE_STATEMENTS = "/api/v2/statements".freeze
      DEFAULT_PARAMETERS = {
        DATE_OUTPUT_FORMAT: "YYYY-MM-DD",
        TIMESTAMP_OUTPUT_FORMAT: "YYYY-MM-DD HH24:MI:SS",
        TIMESTAMP_TZ_OUTPUT_FORMAT: "YYYY-MM-DD HH24:MI:SS TZH",
        TIMESTAMP_NTZ_OUTPUT_FORMAT: "YYYY-MM-DD HH24:MI:SS",
        TIMESTAMP_LTZ_OUTPUT_FORMAT: "YYYY-MM-DD HH24:MI:SS TZH",
        TIME_OUTPUT_FORMAT: "HH24:MI:SS"
      }
      def execute(sql, io = nil, memory_row_limit: 20_000, stats: nil)
        with_debug(sql) do
          resp = connection.post(SNOWFLAKE_STATEMENTS) do |req|
            req.body = {
              statement: sql,
              timeout: config[:query_timeout],
              warehouse: config[:warehouse]&.upcase,
              database: config[:database]&.upcase,
              schema: config[:schema]&.upcase,
              role: config[:role]&.upcase,
              parameters: DEFAULT_PARAMETERS
            }.compact
              .merge(extra_query_params)
              .to_json
          end

          # This will handle all the different response
          # flows. If polling is required it will do that
          # otherwise it will return the parsed response body.
          result = handle_response(resp)
          fetch_data(result, io, memory_row_limit, stats)
        end
      end

      def tables(catalog: nil, schema: nil, database: nil)
        db = database || config[:database]
        sql = "select table_name from #{db}.information_schema.tables"
        where = []
        where << "table_schema='#{schema.upcase}'" if schema
        where << "table_catalog='#{catalog.upcase}'" if catalog

        sql << " where " if where.size > 0
        sql << where.join(" and ")

        res = execute(sql)
        res.flatten
      end

      def metadata(table, catalog: nil, schema: nil, database: nil)
        db_table = Table.new table, schema: schema, catalog: catalog
        db = database || config[:database]
        sql = "select column_name, data_type, numeric_precision, numeric_scale, character_maximum_length from #{db}.information_schema.columns"
        where = ["table_name='#{db_table.physical_name.upcase}'"]
        where << "table_schema='#{db_table.schema.upcase}'" if db_table.schema
        where << "table_catalog='#{db_table.catalog.upcase}'" if db_table.catalog

        sql << " where " if where.size > 0
        sql << where.join(" and ")

        cols = execute(sql)
        cols.each do |col|
          db_table << Column.new(
            name: col[0]&.downcase,
            data_type: col[1]&.downcase,
            precision: col[2]&.downcase,
            scale: col[3]&.downcase,
            max_char_length: col[4]&.downcase
          )
        end

        db_table
      end

      def stats(table, date_column: nil, catalog: nil, schema: nil)
        sql = <<-SQL
                    SELECT count(*) ROW_COUNT
                        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
                        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
                    FROM #{table}
        SQL

        result = execute(sql)

        {
          date_start: result[0][1],
          date_end: result[0][2],
          row_count: result[0][0]
        }
      end

      def expires_at
        @expires_at ||= 1.hour.from_now
      end

      def expired?
        Time.now >= expires_at
      end

      def reset_connection
        @expires_at = nil
        @jwt = nil
        close
      end

      protected

      def fetch_data(result, io = nil, memory_row_limit = nil, stats = nil)
        partitions = result.dig("resultSetMetaData", "partitionInfo")
        stats = validate_and_reset_stats(stats) if stats
        if partitions.size == 1
          if io
            update_stats_and_io(result, stats, io, memory_row_limit)
            io.rewind
            io
          else
            result["data"]
          end
        else
          fetch_partitions(result, stats, io, memory_row_limit)
        end
      end

      def fetch_partitions(result, stats = nil, io = nil, memory_row_limit = nil)
        data = result["data"]
        update_stats_and_io(result, stats, io, memory_row_limit) if io

        partitions = result.dig("resultSetMetaData", "partitionInfo")
        url = "#{SNOWFLAKE_STATEMENTS}/#{result["statementHandle"]}?partition="
        partitions[1..].each.with_index(1) do |_, index|
          logger.debug "Fetching partition #{index} of #{partitions.length - 1} for statement handle: #{result["statementHandle"]}"
          resp = connection.get(url + index.to_s)
          raise ArgumentError.new("Could not data partitions from Snowflake: #{resp.body}") unless resp.status == 200

          part_res = JSON.parse(resp.body)

          if io.nil?
            data = data.concat(part_res["data"])
          else
            update_stats_and_io(part_res, stats, io, memory_row_limit)
          end
        end

        io&.rewind
        io.present? ? io : data
      end

      def update_stats_and_io(result, stats, io, memory_row_limit)
        rows = result["data"]
        return if rows.length == 0

        rows.each do |row|
          update_stats(stats, row, memory_row_limit) if stats
          io << CSV.generate_line(row)
        end
      end

      def handle_response(response)
        # finished running
        result = JSON.parse(response.body)
        if response.status == 200
          result
        elsif response.status == 202
          # need to poll for status
          poll(result)
        else
          msg = result["message"] || result
          raise ArgumentError.new(msg)
        end
      end

      def poll(result)
        logger.debug "Polling snowflake for query status: #{result["statementHandle"]}"
        sleep_time = 0.25

        loop do
          resp = connection.get("#{SNOWFLAKE_STATEMENTS}/#{result["statementHandle"]}")
          poll_result = JSON.parse(resp.body)
          if resp.status == 202
            logger.debug "Polling #{poll_result["statementHandle"]}. Sleeping #{sleep_time}secs..."
            sleep(sleep_time)
            sleep_time = [sleep_time * 2, 30].min
          elsif resp.status == 200
            return poll_result
          else
            msg = poll_result["message"] || poll_result
            raise ArgumentError.new("Could not poll snowflake for status: #{msg}")
          end
        end
      end

      def qualified_username
        "#{account_identifier}.#{config[:username].upcase}"
      end

      def jwt_token
        @jwt ||= JWT.encode({
          iss: "#{qualified_username}.SHA256:#{public_key_fp}",
          sub: qualified_username,
          iat: Time.now.to_i,
          exp: expires_at.to_i # Token is valid for 1 hour
        }, private_key, "RS256")
      end

      def account_identifier
        @account_identifier ||= (config[:account_identifier] || config[:host].split(".").first).upcase
      end

      def private_key
        @private_key ||=
          if File.exist?(config[:private_key])
            OpenSSL::PKey.read(File.read(config[:private_key]))
          else
            config[:private_key]
          end
      end

      def public_key_fp
        @fp ||= Base64.strict_encode64(
          Digest::SHA256.digest(private_key.public_key.to_der)
        )
      end
    end
  end
end
