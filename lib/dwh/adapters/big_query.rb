require 'csv'
require 'timeout'

module DWH
  module Adapters
    # Google BigQuery adapter. Requires the google-cloud-bigquery gem, which is
    # loaded lazily on first connection so the adapter can be created without it.
    #
    # @example Service account keyfile
    #   DWH.create(:bigquery, {
    #     project_id: 'my-gcp-project',
    #     dataset: 'analytics',
    #     keyfile: '/path/to/service-account.json'
    #   })
    #
    # @example Inline service-account credentials (the keyfile's client_email and private_key)
    #   DWH.create(:bigquery, {
    #     project_id: 'my-gcp-project',
    #     dataset: 'analytics',
    #     client_email: 'svc@my-gcp-project.iam.gserviceaccount.com',
    #     private_key: "-----BEGIN PRIVATE KEY-----\n..."
    #   })
    #
    # @example Application Default Credentials (gcloud auth application-default login)
    #   DWH.create(:bigquery, { project_id: 'my-gcp-project', dataset: 'analytics' })
    class BigQuery < Adapter
      config :project_id, String, required: true, message: 'GCP project id'
      config :dataset, String, required: true, message: 'default dataset for unqualified table names'
      config :keyfile, String, required: false, default: nil,
                               message: 'path to service-account JSON; omit to use Application Default Credentials'
      config :client_email, String, required: false, default: nil,
                                    message: 'service-account email from the keyfile; use with private_key instead of keyfile'
      config :private_key, String, required: false, default: nil,
                                   message: 'service-account private key (PEM) from the keyfile; use with client_email'
      config :query_timeout, Integer, required: false, default: 300, message: 'query timeout in seconds'

      # The schema API reports legacy type names; map the ones whose
      # normalized type would otherwise be wrong (INTEGER is 64-bit in BigQuery).
      TYPE_ALIASES = { 'INTEGER' => 'INT64', 'FLOAT' => 'FLOAT64', 'BOOLEAN' => 'BOOL', 'RECORD' => 'STRUCT' }.freeze

      # BigQuery column names, and therefore SELECT aliases, may not contain these
      # characters (https://cloud.google.com/bigquery/docs/schemas#column_names).
      # Dots are left alone so backtick-quoted `dataset.table` paths still work.
      INVALID_NAME_CHARS = %r{[!"$()*,/;?@\[\\\]^{}~]}

      # (see Functions#quote) Replaces characters BigQuery rejects in a column
      # name so planner-generated aliases like "Month(Post Date)" execute.
      def quote(exp)
        super(exp.to_s.gsub(INVALID_NAME_CHARS, '_'))
      end

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        require 'google/cloud/bigquery'
        opts = { project_id: config[:project_id] }
        creds = google_credentials
        opts[:credentials] = creds if creds
        @connection = Google::Cloud::Bigquery.new(**opts, **extra_connection_params)
      rescue LoadError
        raise ConfigError, <<~MSG
          BigQuery adapter requires the 'google-cloud-bigquery' gem.

          Install with: gem install google-cloud-bigquery

          No system libraries required (pure Ruby).
        MSG
      rescue StandardError => e
        raise ConfigError, "Failed to connect to BigQuery: #{e.message}"
      end

      # (see Adapter#close) The Google client holds no persistent connection,
      # so there is nothing to close; just drop the reference.
      def close
        @connection = nil
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        raise ConnectionError, "Dataset '#{config[:dataset]}' not found" unless connection.dataset(config[:dataset])

        true
      rescue StandardError => e
        raise ConnectionError, "BigQuery connection test failed: #{e.message}" if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables(**qualifiers)
        dataset_for(qualifiers).tables.all.map(&:table_id)
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        sql = 'SELECT COUNT(*) AS row_count'
        sql += ", MIN(#{date_column}) AS date_start, MAX(#{date_column}) AS date_end" if date_column
        row = execute("#{sql} FROM `#{dataset_name(qualifiers)}.#{table}`", format: :object).first || {}

        TableStats.new(row_count: row['row_count'], date_start: row['date_start'], date_end: row['date_end'])
      end

      # (see Adapter#metadata)
      # Uses the table schema API instead of INFORMATION_SCHEMA: one call, and
      # precision/scale/length come back as attributes rather than parsed from strings.
      def metadata(table, **qualifiers)
        db_table = Table.new table, schema: dataset_name(qualifiers)
        bq_table = dataset_for(qualifiers).table(db_table.physical_name)
        raise ExecutionError, "Table '#{db_table.schema}.#{db_table.physical_name}' not found" unless bq_table

        bq_table.schema.fields.each do |field|
          db_table << Column.new(
            name: field.name,
            data_type: TYPE_ALIASES.fetch(field.type, field.type),
            precision: field.precision || 0,
            scale: field.scale || 0,
            max_char_length: field.max_length
          )
        end

        db_table
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        result = with_debug(sql) { with_retry(retries) { run_query(sql) } }

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array then result[:rows]
        when :object then result[:rows].map { |row| Hash[result[:headers].zip(row)] }
        when :csv then rows_to_csv(result[:headers], result[:rows])
        when :native then result
        else raise UnsupportedCapability, "Unsupported format: #{format} for BigQuery adapter"
        end
      end

      # (see Adapter#execute_stream)
      def execute_stream(sql, io, stats: nil, retries: 0)
        with_debug(sql) do
          with_retry(retries) do
            data = query_data(sql)
            io.write(CSV.generate_line(headers_of(data)))
            data.all.each do |row|
              values = row.values
              stats << values unless stats.nil?
              io.write(CSV.generate_line(values))
            end
          end
        end

        io.rewind
        io
      end

      # (see Adapter#stream)
      def stream(sql, &block)
        with_debug(sql) { query_data(sql).all.each { |row| block.call(row.values) } }
      end

      private

      # Inline service-account fields win over a keyfile path. Neither present
      # means Application Default Credentials.
      def google_credentials
        if config[:client_email].to_s != '' && config[:private_key].to_s != ''
          { 'type' => 'service_account', 'project_id' => config[:project_id],
            'client_email' => config[:client_email], 'private_key' => config[:private_key] }
        elsif config[:keyfile].to_s != ''
          config[:keyfile]
        end
      end

      def dataset_name(qualifiers)
        qualifiers[:dataset] || qualifiers[:schema] || config[:dataset]
      end

      def dataset_for(qualifiers)
        name = dataset_name(qualifiers)
        connection.dataset(name) || raise(ExecutionError, "Dataset '#{name}' not found")
      end

      # Runs the query and returns the gem's paged Data object. Rows are hashes
      # keyed by column name in column order, so row.values matches data.fields.
      def query_data(sql)
        Timeout.timeout(config[:query_timeout]) do
          connection.query(sql, dataset: config[:dataset], project: config[:project_id])
        end
      rescue DWHError
        raise
      rescue StandardError => e
        raise ExecutionError, "BigQuery query failed: #{e.message}"
      end

      def run_query(sql)
        data = query_data(sql)
        { headers: headers_of(data), rows: data.all.map(&:values) }
      end

      # DDL/DML statements return no schema.
      def headers_of(data)
        data.schema&.fields&.map(&:name) || []
      end

      def rows_to_csv(headers, rows)
        CSV.generate do |csv|
          csv << headers
          rows.each { |row| csv << row }
        end
      end
    end
  end
end
