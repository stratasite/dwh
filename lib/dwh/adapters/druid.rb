module DWH
  module Adapters
    # (include Adapter)
    class Druid < Adapter
      DRUID_DATASOURCES = '/druid/coordinator/v1/datasources'.freeze
      DRUID_SQL = '/druid/v2/sql/'.freeze
      COLUMNS_FOR_TABLE = '"COLUMN_NAME","DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", "CHARACTER_MAXIMUM_LENGTH"'.freeze

      config :protocol, String, required: true, default: 'http', message: 'must be http or https'
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: true, message: 'port to connect to'
      config :query_timeout, Integer, required: false, default: 600, message: 'query execution timeout in seconds'

      def connection
        @connection ||= Faraday.new(
          url: "#{config[:protocol]}://#{config[:host]}:#{config[:port]}",
          headers: {
            'Content-Type' => 'application/json'
          },
          request: {
            timeout: config[:query_timeout]
          }.merge(extra_connection_params)
        )
      end

      def tables(catalog: nil, schema: nil)
        resp = connection.get(DRUID_DATASOURCES) do |req|
          req.options.timeout = 30
        end
        JSON.parse resp.body
      end

      def stats(table, date_column: '__time', catalog: nil, schema: nil)
        sql = <<-SQL
                    SELECT min(#{date_column}) DATE_START, max(__time) DATE_END, count(*) ROW_COUNT
                    FROM "#{table}"
        SQL

        result = execute(sql)

        TableStats.new(
          row_count: result[0][2],
          date_start: result[0][0],
          date_end: result[0][1]
        )
      end

      def drop_unused_segments(table, interval)
        url = "/druid/coordinator/v1/datasources/#{table}/markUnused"

        logger.debug '=== Dropping Segments ==='

        response = connection.post(url) do |req|
          req.headers['Content-Type'] = 'application/json'
          req.body = { interval: interval }.to_json
        end

        logger.debug response.status
      end

      def metadata(table)
        sql = <<-SQL
                    SELECT #{COLUMNS_FOR_TABLE} FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '#{table}'
        SQL

        stats = stats(table)
        db_table = Table.new 'table', **stats
        cols = execute(sql, 'object')
        st = table_druid_schema_types(table, stats[:date_end])

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

      def execute(sql, format: 'array', retries: 0)
        format = format == 'native' ? 'array' : format
        resp = with_debug(sql) do
          with_retry(retries) do
            connection.post(DRUID_SQL) do |req|
              req.headers['Content-Type'] = 'application/json'
              req.body = {
                query: sql,
                resultFormat: format,
                context: { sqlTimeZone: 'Etc/UTC' }
              }.merge(extra_query_params)
                         .to_json
            end
          end
        end

        raise ExecutionError, "Could not execute #{sql}: \n #{resp.body}" if resp.status != 200

        JSON.parse(resp.body)
      end

      def execute_stream(sql, io, memory_row_limit: 20_000, stats: nil, retries: 0)
        rows = []
        stats = validate_and_reset_stats(stats)

        resp = with_debug(sql) do
          with_retry(retries) do
            connection.post(DRUID_SQL) do |req|
              req.headers['Content-Type'] = 'application/json'
              req.body = { query: sql, resultFormat: 'csv' }
              # added timezone here due to druid bug
              # where date sub query joins failed without it.
              # context: { sqlTimeZone: 'Etc/UTC'}.merge(extra_query_params).to_json

              parseable_row = ''
              req.options.on_data = proc do |chunk, chunk_size|
                handle_streaming_chunk(chunk, chunk_size, io, stats, rows, parseable_row, memory_row_limit)
              end
            end
          end
        end

        if stats
          stats.rows = rows
          stats.total_rows = stats.limit if stats.limit && stats.total_rows > stats.limit
        end

        io.rewind
        # Raise exception on failed runs
        raise ExecutionError.new(io.read) unless resp.success?

        io
      end

      def stream(sql, &block)
        on_data_calls = 0
        with_debug(sql) do
          connection.post(DRUID_SQL) do |req|
            req.headers['Content-Type'] = 'application/json'
            req.body = { query: sql, resultFormat: 'csv' }.to_json
            # added timezone here due to druid bug
            # where date sub query joins failed without it.
            # context: { sqlTimeZone: 'Etc/UTC'}.merge(extra_query_params).to_json

            req.options.on_data = proc do |chunk, _chunk_size|
              block.call chunk.force_encoding('utf-8')
              on_data_calls += 1
            end
          end
        end

        on_data_calls
      end

      protected

      def table_druid_schema_types(table, last_date)
        start_date = Date.parse(last_date) - 1
        url_friendly_interval = "#{start_date}_#{last_date}"
        url = "/druid/coordinator/v1/datasources/#{table}/intervals/#{url_friendly_interval}?full"

        resp = connection.get(url) do |req|
          req.options.timeout = 30
        end

        raise ArgumentError, "Could not fetch druid schema types: \n #{resp.body}" if resp.status != 200

        res = JSON.parse(resp.body)
        meta = res.flatten[1].flatten(4)[1]['metadata']
        {
          dimensions: meta['dimensions'].split(','),
          metrics: meta['metrics'].split(',')
        }
      end

      def handle_streaming_chunk(chunk, chunk_size, io, stats, rows, parseable_row, memory_row_limit = 20_000)
        io.write chunk.force_encoding('utf-8')
        update_streaming_stats(stats, chunk_size, chunk.lines.length) if stats

        parseable_row += chunk
        process_streaming_rows(parseable_row, rows, chunk) if rows.length <= memory_row_limit
      end

      def update_streaming_stats(stats, chunk_size, line_count)
        stats.max_page_size = [chunk_size, stats.max_page_size].max
        stats.total_rows += line_count
      end

      def process_streaming_rows(parseable_row, rows, chunk)
        rows.concat(CSV.parse(parseable_row, skip_blanks: true))
        parseable_row.clear
      rescue CSV::MalformedCSVError
        logger.debug("Unparseable:\n #{chunk}")
      end
    end
  end
end
