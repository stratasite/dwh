module DWH
  module Adapters
    # SQLite adapter optimized for analytical workloads.
    #
    # This requires the ruby {https://github.com/sparklemotion/sqlite3-ruby sqlite3} gem.
    #
    # Generally, adapters should be created using {DWH::Factory#create DWH.create}. Where a configuration
    # is passed in as options hash or argument list.
    #
    # @example Basic connection with required only options
    #   DWH.create(:sqlite, {file: 'path/to/my/database.db' })
    #
    # @example Open in read only mode
    #   DWH.create(:sqlite, {file: 'path/to/my/database.db', readonly: true})
    #
    # @example Configure with custom performance pragmas
    #   DWH.create(:sqlite, {file: 'path/to/my/database.db',
    #     pragmas: { cache_size: -128000, mmap_size: 268435456 }})
    #
    # @note This adapter enables WAL mode by default for better concurrent read performance.
    #   Set `enable_wal: false` to disable this behavior.
    class Sqlite < Adapter
      config :file, String, required: true, message: 'path/to/sqlite/db'
      config :readonly, Boolean, required: false, default: false, message: 'open database in read-only mode'
      config :enable_wal, Boolean, required: false, default: true, message: 'enable WAL mode for better concurrency'
      config :pragmas, Hash, required: false, message: 'hash of PRAGMA statements for performance tuning'
      config :timeout, Integer, required: false, default: 5000, message: 'busy timeout in milliseconds'

      # Default pragmas optimized for analytical workloads
      DEFAULT_PRAGMAS = {
        cache_size: -64_000, # 64MB cache (negative means KB)
        temp_store: 'MEMORY', # Store temp tables in memory
        mmap_size: 134_217_728, # 128MB memory-mapped I/O
        page_size: 4096,         # Standard page size
        synchronous: 'NORMAL'    # Faster than FULL, safe with WAL
      }.freeze

      # (see Adapter#connection)
      def connection
        return @connection if @connection

        options = build_open_options
        @connection = SQLite3::Database.new(config[:file], options)

        # Set busy timeout to handle concurrent access
        @connection.busy_timeout(config[:timeout])

        # Don't return results as hash by default for performance
        @connection.results_as_hash = false

        # Enable WAL mode for concurrent reads (unless disabled or readonly)
        @connection.execute('PRAGMA journal_mode = WAL') if config[:enable_wal] && !config[:readonly]

        # Apply default pragmas
        apply_pragmas(DEFAULT_PRAGMAS)

        # Apply user-specified pragmas (will override defaults)
        apply_pragmas(config[:pragmas]) if config.key?(:pragmas)

        @connection
      rescue StandardError => e
        raise ConfigError, e.message
      end

      # (see Adapter#close)
      def close
        return if @connection.nil?

        @connection.close unless @connection.closed?
        @connection = nil
      end

      # (see Adapter#test_connection)
      def test_connection(raise_exception: false)
        connection
        connection.execute('SELECT 1')
        true
      rescue StandardError => e
        raise ConnectionError, e.message if raise_exception

        false
      end

      # (see Adapter#tables)
      def tables(**qualifiers)
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"

        res = execute(sql)
        res.flatten
      end

      # (see Adapter#stats)
      def stats(table, date_column: nil, **qualifiers)
        db_table = Table.new table, **qualifiers

        sql = <<-SQL
        SELECT count(*) AS ROW_COUNT
        #{date_column.nil? ? '' : ", min(#{date_column}) AS DATE_START"}
        #{date_column.nil? ? '' : ", max(#{date_column}) AS DATE_END"}
        FROM #{db_table.physical_name}
        SQL

        result = execute(sql)
        TableStats.new(
          row_count: result.first[0],
          date_start: date_column ? result.first[1] : nil,
          date_end: date_column ? result.first[2] : nil
        )
      end

      # (see Adapter#metadata)
      def metadata(table, **qualifiers)
        db_table = Table.new table, **qualifiers

        # SQLite uses PRAGMA table_info for metadata
        sql = "PRAGMA table_info(#{db_table.physical_name})"

        cols = execute(sql)
        cols.each do |col|
          # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
          db_table << Column.new(
            name: col[1],
            data_type: col[2],
            precision: nil,
            scale: nil,
            max_char_length: nil
          )
        end

        db_table
      end

      # (see Adapter#execute)
      def execute(sql, format: :array, retries: 0)
        begin
          result = with_debug(sql) { with_retry(retries) { connection.execute(sql) } }
        rescue StandardError => e
          raise ExecutionError, e.message
        end

        format = format.downcase if format.is_a?(String)
        case format.to_sym
        when :array
          result
        when :object
          result_to_hash(sql, result)
        when :csv
          result_to_csv(sql, result)
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
            stmt = connection.prepare(sql)
            columns = stmt.columns

            io.write(CSV.generate_line(columns))

            stmt.execute.each do |row|
              stats << row unless stats.nil?
              io.write(CSV.generate_line(row))
            end

            stmt.close
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
          stmt = connection.prepare(sql)
          stmt.execute.each do |row|
            block.call(row)
          end
          stmt.close
        end
      end

      # Custom date truncation implementation. SQLite doesn't offer
      # a native DATE_TRUNC function. We use 'start of' modifiers
      # for year, month, and day, and custom logic for quarter and week.
      # @see Dates#truncate_date
      def truncate_date(unit, exp)
        unit = unit.strip.downcase

        case unit
        when 'year'
          "date(#{exp}, 'start of year')"
        when 'quarter'
          # Calculate quarter start using CASE statement
          # Q1: Jan-Mar (months 1-3) -> start of year
          # Q2: Apr-Jun (months 4-6) -> start of year + 3 months
          # Q3: Jul-Sep (months 7-9) -> start of year + 6 months
          # Q4: Oct-Dec (months 10-12) -> start of year + 9 months
          '(CASE ' \
          "WHEN CAST(strftime('%m', #{exp}) AS INTEGER) BETWEEN 1 AND 3 THEN date(#{exp}, 'start of year') " \
          "WHEN CAST(strftime('%m', #{exp}) AS INTEGER) BETWEEN 4 AND 6 THEN date(#{exp}, 'start of year', '+3 months') " \
          "WHEN CAST(strftime('%m', #{exp}) AS INTEGER) BETWEEN 7 AND 9 THEN date(#{exp}, 'start of year', '+6 months') " \
          "ELSE date(#{exp}, 'start of year', '+9 months') " \
          'END)'
        when 'month'
          "date(#{exp}, 'start of month')"
        when 'week'
          # Use week start day from settings
          gsk("#{settings[:week_start_day].downcase}_week_start_day")
            .gsub(/@exp/i, exp)
        when 'day', 'date'
          "date(#{exp})"
        when 'hour'
          # SQLite datetime returns timestamp, truncate to hour
          "datetime(strftime('%Y-%m-%d %H:00:00', #{exp}))"
        when 'minute'
          "datetime(strftime('%Y-%m-%d %H:%M:00', #{exp}))"
        when 'second'
          "datetime(strftime('%Y-%m-%d %H:%M:%S', #{exp}))"
        else
          raise UnsupportedCapability, "Currently not supporting truncation at #{unit} level"
        end
      end

      # SQLite's strftime doesn't support %A (day name) or %B (month name)
      # We need to implement these using CASE statements based on day/month numbers
      def extract_day_name(exp, abbreviate: false)
        day_num = "CAST(strftime('%w', #{exp}) AS INTEGER)"

        if abbreviate
          # Abbreviated day names: SUN, MON, TUE, etc.
          "(CASE #{day_num} " \
          "WHEN 0 THEN 'SUN' " \
          "WHEN 1 THEN 'MON' " \
          "WHEN 2 THEN 'TUE' " \
          "WHEN 3 THEN 'WED' " \
          "WHEN 4 THEN 'THU' " \
          "WHEN 5 THEN 'FRI' " \
          "WHEN 6 THEN 'SAT' " \
          'END)'
        else
          # Full day names: SUNDAY, MONDAY, TUESDAY, etc.
          "(CASE #{day_num} " \
          "WHEN 0 THEN 'SUNDAY' " \
          "WHEN 1 THEN 'MONDAY' " \
          "WHEN 2 THEN 'TUESDAY' " \
          "WHEN 3 THEN 'WEDNESDAY' " \
          "WHEN 4 THEN 'THURSDAY' " \
          "WHEN 5 THEN 'FRIDAY' " \
          "WHEN 6 THEN 'SATURDAY' " \
          'END)'
        end
      end

      def extract_month_name(exp, abbreviate: false)
        month_num = "CAST(strftime('%m', #{exp}) AS INTEGER)"

        if abbreviate
          # Abbreviated month names: JAN, FEB, MAR, etc.
          "(CASE #{month_num} " \
          "WHEN 1 THEN 'JAN' " \
          "WHEN 2 THEN 'FEB' " \
          "WHEN 3 THEN 'MAR' " \
          "WHEN 4 THEN 'APR' " \
          "WHEN 5 THEN 'MAY' " \
          "WHEN 6 THEN 'JUN' " \
          "WHEN 7 THEN 'JUL' " \
          "WHEN 8 THEN 'AUG' " \
          "WHEN 9 THEN 'SEP' " \
          "WHEN 10 THEN 'OCT' " \
          "WHEN 11 THEN 'NOV' " \
          "WHEN 12 THEN 'DEC' " \
          'END)'
        else
          # Full month names: JANUARY, FEBRUARY, MARCH, etc.
          "(CASE #{month_num} " \
          "WHEN 1 THEN 'JANUARY' " \
          "WHEN 2 THEN 'FEBRUARY' " \
          "WHEN 3 THEN 'MARCH' " \
          "WHEN 4 THEN 'APRIL' " \
          "WHEN 5 THEN 'MAY' " \
          "WHEN 6 THEN 'JUNE' " \
          "WHEN 7 THEN 'JULY' " \
          "WHEN 8 THEN 'AUGUST' " \
          "WHEN 9 THEN 'SEPTEMBER' " \
          "WHEN 10 THEN 'OCTOBER' " \
          "WHEN 11 THEN 'NOVEMBER' " \
          "WHEN 12 THEN 'DECEMBER' " \
          'END)'
        end
      end

      # SQLite's CAST(... AS DATE) doesn't work properly - it just extracts the year
      # We need to override cast to use the date() function for DATE types
      def cast(exp, type)
        if type.to_s.downcase == 'date'
          "date(#{exp})"
        else
          super
        end
      end

      def valid_config?
        super
        require 'sqlite3'
      rescue LoadError
        raise ConfigError, "Required 'sqlite3' gem missing. Please add it to your Gemfile."
      end

      private

      def build_open_options
        options = {}
        options[:readonly] = true if config[:readonly]
        options
      end

      def apply_pragmas(pragmas)
        return unless pragmas

        pragmas.each do |pragma, value|
          # Format value appropriately (quote strings, leave numbers/keywords as-is)
          formatted_value = value.is_a?(String) && value.upcase != value ? "'#{value}'" : value
          @connection.execute("PRAGMA #{pragma} = #{formatted_value}")
        end
      end

      def result_to_hash(sql, result)
        return [] if result.empty?

        # Get column names by preparing statement
        stmt = connection.prepare(sql)
        columns = stmt.columns
        stmt.close

        result.map do |row|
          columns.zip(row).to_h
        end
      end

      def result_to_csv(sql, result)
        # Get column names by preparing statement
        stmt = connection.prepare(sql)
        columns = stmt.columns
        stmt.close

        CSV.generate do |csv|
          csv << columns
          result.each do |row|
            csv << row
          end
        end
      end
    end
  end
end
