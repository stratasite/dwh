module DWH
  module Adapters
    # Druid adapter.
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
    class Druid < Adapter
      DRUID_STATUS = '/status'.freeze
      DRUID_DATASOURCES = '/druid/coordinator/v1/datasources'.freeze
      DRUID_SQL = '/druid/v2/sql/'.freeze
      COLUMNS_FOR_TABLE = '"COLUMN_NAME","DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", "CHARACTER_MAXIMUM_LENGTH"'.freeze

      config :protocol, String, required: true, default: 'http', message: 'must be http or https', allowd: %w[http https]
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: true, default: 8081, message: 'port to connect to'
      config :query_timeout, Integer, required: false, default: 600, message: 'query execution timeout in seconds'
      config :open_timeout, Integer, required: false, default: nil, message: 'how long to wait to connect'
      config :client_name, String, default: 'DWH Ruby Gem', message: 'client_name will be passed in the context object'
      config :basic_auth, String, required: false, message: 'authorization key sent in the header'

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        @connection = Faraday.new(
          url: "#{config[:protocol]}://#{config[:host]}:#{config[:port]}",
          headers: {
            'Content-Type' => 'application/json',
            **(config[:basic_auth] ? { 'Authorization' => "Basic #{config[:basic_auth]}" } : {})
          },
          request: {
            timeout: config[:query_timeout],
            open_timeout: config[:open_timeout],
            context: {
              client_name: config[:client_name]
            }
          }.merge(extra_connection_params)
        )

        @connection
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        res = connection.get(DRUID_STATUS)
        unless res.success?
          raise ConnectionError, res.body if raise_exception

          false
        end

        true
      rescue Faraday::ConnectionFailed => e
        raise ConnectionError, e.message if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables
        resp = connection.get(DRUID_DATASOURCES) do |req|
          req.options.timeout = 30
        end
        JSON.parse resp.body
      end

      # Date column will default to __time. If the datasource,
      # does not have a date column please set it to nil
      # @param table [String] table name
      # @param date_column [String] optional date column
      # @see Adapter#stats
      def stats(table, date_column: '__time')
        sql = <<-SQL
        SELECT
        count(*) ROW_COUNT
        #{date_column.nil? ? nil : ", min(#{date_column}) DATE_START"}
        #{date_column.nil? ? nil : ", max(#{date_column}) DATE_END"}
        FROM "#{table}"
        SQL

        result = execute(sql)

        TableStats.new(
          row_count: result[0][0],
          date_start: result[0][1],
          date_end: result[0][2]
        )
      end

      # Marks unused segments of a datasource/table as unused
      # @param table [String] datasource/table name
      # @param interval [String] date interval in the format of from_date/to_date
      #   as valid ISO timestamps
      def drop_unused_segments(table, interval)
        url = "/druid/coordinator/v1/datasources/#{table}/markUnused"

        logger.debug '=== Dropping Segments ==='

        response = connection.post(url) do |req|
          req.headers['Content-Type'] = 'application/json'
          req.body = { interval: interval }.to_json
        end

        logger.debug response.status
      end

      # (see Adapter#metadata)
      def metadata(table)
        sql = <<-SQL
        SELECT #{COLUMNS_FOR_TABLE} FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '#{table}'
        SQL

        stats = stats(table)
        db_table = Table.new 'table', table_stats: stats
        cols = execute(sql, format: :object)
        st = table_druid_schema_types(table, stats.date_end)

        cols.each do |col|
          db_table << Column.new(
            name: col['COLUMN_NAME'],
            schema_type: st[:metrics].include?(col['COLUMN_NAME']) ? 'measure' : 'dimension',
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
        format = format.to_sym
        result_format = format == :native ? 'array' : format.to_s
        resp = with_debug(sql) do
          with_retry(retries) do
            connection.post(DRUID_SQL) do |req|
              req.headers['Content-Type'] = 'application/json'
              req.body = {
                query: sql,
                resultFormat: result_format,
                context: { sqlTimeZone: 'Etc/UTC' }
              }.merge(extra_query_params)
                         .to_json
            end
          end
        end

        raise ExecutionError, "Could not execute #{sql}: \n #{resp.body}" if resp.status != 200

        if format == :native
          resp
        else
          format == :csv ? resp.body : JSON.parse(resp.body)
        end
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 0)
        resp = with_debug(sql) do
          with_retry(retries) do
            connection.post(DRUID_SQL) do |req|
              req.headers['Content-Type'] = 'application/json'
              req.body = {
                query: sql,
                resultFormat: 'csv'
                # added timezone here due to druid bug
                # where date sub query joins failed without it.
                # context: { sqlTimeZone: 'Etc/UTC'}
              }.merge(extra_query_params).to_json

              parseable_row = ''
              req.options.on_data = proc do |chunk, _|
                handle_streaming_chunk(io, chunk, stats, parseable_row)
              end
            end
          end
        end

        io.rewind
        # Raise exception on failed runs
        raise ExecutionError, io.read unless resp.success?

        io
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        on_data_calls = 0
        with_debug(sql) do
          connection.post(DRUID_SQL) do |req|
            req.headers['Content-Type'] = 'application/json'
            req.body = { query: sql, resultFormat: 'csv' }.to_json
            req.options.on_data = proc do |chunk, _chunk_size|
              block.call chunk.force_encoding('utf-8')
              on_data_calls += 1
            end
          end
        end

        on_data_calls
      end

      protected

      def table_druid_schema_types(table, last_interval_start_date)
        end_date = last_interval_start_date + 1
        start_date = last_interval_start_date
        url_friendly_interval = "#{start_date.strftime('%Y-%m-%d')}_#{end_date.strftime('%Y-%m-%d')}"
        url = "/druid/coordinator/v1/datasources/#{table}/intervals/#{url_friendly_interval}?full"

        resp = connection.get(url) do |req|
          req.options.timeout = 30
        end

        raise ExecutionError, "Could not fetch druid schema types: \n #{resp.body}" if resp.status != 200

        res = JSON.parse(resp.body)
        meta = res.flatten[1].flatten(4)[1]['metadata']
        {
          dimensions: meta['dimensions'].split(','),
          metrics: meta['metrics'].split(',')
        }
      end

      def handle_streaming_chunk(io, chunk, stats, parseable_row)
        io.write chunk.rstrip.force_encoding('utf-8')

        parseable_row += chunk
        process_streaming_rows(parseable_row, chunk, stats)
      end

      def process_streaming_rows(parseable_row, chunk, stats)
        return if stats.nil? || stats&.limit_reached?

        rows = CSV.parse(parseable_row, skip_blanks: true)
        rows.each { |row| stats << row }
        parseable_row.clear
      rescue CSV::MalformedCSVError
        logger.debug("Unparseable:\n #{chunk}")
      end
    end
  end
end
