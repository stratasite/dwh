module DWH
  module Adapters
    # ClickHouse adapter for executing analytical queries against ClickHouse databases.
    # Uses the ClickHouse HTTP interface (default port 8123) via Faraday.
    #
    # @example Basic local connection
    #   DWH.create(:clickhouse, { host: 'localhost', database: 'default' })
    #
    # @example With authentication
    #   DWH.create(:clickhouse, {
    #     host: 'my-clickhouse.example.com',
    #     port: 8443,
    #     protocol: 'https',
    #     database: 'analytics',
    #     username: 'analyst',
    #     password: 'secret'
    #   })
    class ClickHouse < Adapter
      QUERY_FORMAT = 'JSONCompact'.freeze

      config :protocol, String, required: false, default: 'http', message: 'http or https'
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: false, default: 8123, message: 'ClickHouse HTTP interface port (default 8123)'
      config :database, String, required: false, default: 'default', message: 'database to connect to'
      config :username, String, required: false, default: 'default', message: 'username (default: default)'
      config :password, String, required: false, default: nil, message: 'password'
      config :query_timeout, Integer, required: false, default: 300, message: 'query execution timeout in seconds'

      def connection
        return @connection if @connection

        headers = {
          'Content-Type' => 'text/plain',
          'X-ClickHouse-User' => config[:username],
          'X-ClickHouse-Database' => database
        }
        headers['X-ClickHouse-Key'] = config[:password] if config[:password]

        @connection = Faraday.new(
          url: "#{config[:protocol]}://#{config[:host]}:#{config[:port]}",
          headers: headers,
          request: { timeout: config[:query_timeout] }
        )

        @connection
      end

      def test_connection(raise_exception: false)
        res = connection.get('/ping')
        unless res.success? && res.body.strip == 'Ok.'
          raise ConnectionError, "ClickHouse ping returned: #{res.body}" if raise_exception

          return false
        end
        true
      rescue Faraday::ConnectionFailed => e
        raise ConnectionError, e.message if raise_exception

        false
      end

      def tables(**qualifiers)
        db = qualifiers[:schema] || database
        sql = "SELECT name FROM system.tables WHERE database = '#{db}' AND engine NOT IN ('View', 'MaterializedView')"
        execute_raw(sql)['data'].flatten
      end

      def metadata(table, **qualifiers)
        db = qualifiers[:schema] || database
        full_table = db ? "#{db}.#{table}" : table
        # DESCRIBE returns: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
        res = execute_raw("DESCRIBE TABLE #{full_table}")
        db_table = Table.new(table, schema: db)
        res['data'].each do |row|
          db_table << Column.new(name: row[0], data_type: row[1])
        end
        db_table
      end

      def stats(table, date_column: nil, **qualifiers)
        db = qualifiers[:schema] || database
        full_table = db ? "#{db}.#{table}" : table
        sql = +'SELECT count() AS row_count'
        sql << ", min(#{date_column}) AS date_start, max(#{date_column}) AS date_end" if date_column
        sql << " FROM #{full_table}"

        row = execute_raw(sql)['data'][0]
        TableStats.new(
          row_count: row[0].to_i,
          date_start: date_column ? safe_parse_date(row[1]) : nil,
          date_end: date_column ? safe_parse_date(row[2]) : nil
        )
      end

      def execute(sql, format: :array, retries: 0)
        raw = with_debug(sql) { with_retry(retries) { execute_raw(sql) } }
        format_result(raw, format)
      rescue ExecutionError
        raise
      rescue StandardError => e
        raise ExecutionError, e.message
      end

      def execute_stream(sql, io, stats: nil, retries: 0)
        with_debug(sql) do
          with_retry(retries) do
            raw = execute_raw(sql)
            cols = raw['meta'].map { it['name'] }
            io.write(CSV.generate_line(cols))
            raw['data'].each do |row|
              stats << row unless stats.nil?
              io.write(CSV.generate_line(row))
            end
          end
        end
        io.rewind
        io
      rescue ExecutionError
        raise
      rescue StandardError => e
        raise ExecutionError, e.message
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        with_debug(sql) do
          execute_raw(sql)['data'].each(&block)
        end
      end

      private

      def execute_raw(sql)
        resp = connection.post('/') do |req|
          req.body = "#{sql} FORMAT #{QUERY_FORMAT}"
        end
        raise ExecutionError, "ClickHouse error: #{resp.body}" unless resp.success?

        JSON.parse(resp.body)
      rescue Faraday::Error => e
        raise ExecutionError, e.message
      end

      def format_result(raw, format)
        case format.to_sym
        when :array
          raw['data']
        when :object
          cols = raw['meta'].map { it['name'] }
          raw['data'].map { |row| cols.zip(row).to_h }
        when :csv
          CSV.generate do |csv|
            csv << raw['meta'].map { it['name'] }
            raw['data'].each { |row| csv << row }
          end
        when :native
          raw
        else
          raise ArgumentError, "Unsupported format: #{format}"
        end
      end

      def safe_parse_date(val)
        return nil if val.nil? || val.to_s.empty?

        Date.parse(val.to_s)
      rescue Date::Error
        nil
      end
    end
  end
end
