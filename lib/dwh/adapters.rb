require "csv"
require_relative "settings"
require_relative "capabilities"
require_relative "functions"
require_relative "behaviors"
require_relative "logger"

module DWH
  module Adapters
    class Adapter
      extend Settings
      include Capabilities
      include Functions
      include Behaviors
      include Logger

      def self.define_config(name, options = {})
        @config_definitions ||= {}
        @config_definitions[name.to_sym] = {
          required: options[:required] || false,
          default: options[:default],
          message: options[:message] || "Invalid or missing parameter: #{name}"
        }
      end

      def self.get_config_definitions
        @config_definitions || {}
      end

      attr_reader :config

      def initialize(config)
        @config = config.transform_keys(&:to_sym)
        # Per instance customization of general settings
        # So you can have multiple connections to Trino
        # but exhibit diff behavior
        @settings = self.class.adapter_settings.merge(
          (config[:settings] || {}).transform_keys(&:to_sym)
        )

        valid_config?
      end

      attr_reader :settings

      def alter_settings(changes = {})
        @settings.merge!(changes)
      end

      def connection
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      def set_user_context(user: nil, password: nil, timeout: nil)
        logger.debug "#{adapter_name} does not support changing connection context of an active connection."
      end

      def reset_user_context
        false
      end

      def close
        @connection.close if @connection
      end

      # Execute sql on db
      #
      # @param sql [String] actual sql
      # @return [Array[]]
      def execute(sql)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Execute sql and stream responses back.
      #
      # @param sql [String] - actual sql
      # @param io [IO] - IO object to write responses to
      # @param memory_row_limit [Integer] max number of rows to collect in memory - default 20_0000
      # @param stats - Object to manage retrieved data limits and stats
      #   expected methods of stats
      #       rows -          [Array[]] of results upto limit
      #       total_rows -    [Integer] sets total row count.. increments
      #       max_page_size - [Integer] sets the max byte size of 50 rows
      # @return [IO]
      def execute_stream(sql, io, memory_row_limit: 20000, stats: nil)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Returns basic stats of a given table.  Will typically include row_count,
      # date_start, and date_end.
      #
      # In druid or tables where we know the partitioning date column we will
      # find the min max dates along with date range.
      #
      # @param table [String] - table name
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [Hash]
      # ==
      #   row_count [Integer]
      #   date_start [Date] - min date
      #   date_end [Date] - max date
      #
      # Example:
      #   row_count("public.big_table", date_column: "fact_date")
      #   row_count("big_table")
      #   row_count("big_table",schema: "public")
      def stats(table, date_column: nil, catalog: nil, schema: nil)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Get all tables available in the
      # target db.  It will use the default catalog and schema
      # config only specified here.
      #
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [Array<String>]
      def tables(catalog: nil, schema: nil)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Check if table exists in remote db.
      #
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [Boolean]
      def has_table?(table, catalog: nil, schema: nil)
        tables(catalog: catalog, schema: schema).include?(table)
      end

      # Get the schema structure of a given a given table_name.
      # Pass in optional catalog and schema info.
      #
      # @param table [String] - table name
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [Adapters::Db::Schema::DbTable]
      #
      # Example:
      #   metadata("public.big_table")
      #   metadata("big_table")
      #   metadata("big_table",schema: "public")
      def metadata(table, catalog: nil, schema: nil)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      def with_retry(max_attempts = 2, &block)
        attempts = 0

        begin
          attempts += 1
          yield
        rescue => e
          if attempts < max_attempts
            logger.warn "Attempt #{attempts} failed with error: #{e.message}. Retrying..."
            retry
          else
            logger.error "Failed after #{attempts} attempts with error: #{e.message}"
            raise
          end
        end
      end

      def with_debug(sql, &block)
        logger.debug("=== SQL === \n#{sql}")
        result = yield
        logger.debug("=== FINISHED SQL ===")

        result
      end

      def adapter_name
        self.class.name.demodulize
      end

      def extra_connection_params
        config[:extra_connection_params] || {}
      end

      def extra_query_params
        config[:extra_query_params] || {}
      end

      protected

      # Update the stats object as rows stream in.
      # Captures the max row size in bytes
      def update_stats(stats, row, memory_row_limit)
        return unless stats

        if stats.rows.length <= memory_row_limit
          stats.rows << row
        end

        stats.total_rows += 1
        stats.max_page_size = [row.to_s.bytesize, stats.max_page_size].max
      end

      # Used for streaming executions. Reset
      # stats to 0 to start
      def validate_and_reset_stats(stats)
        return if stats.nil?

        [:rows, :total_rows, :max_page_size].each do |key|
          unless stats.respond_to?(key)
            raise ArgumentError.new("#{key} is missing in stats object.")
          end
        end

        stats.rows = []
        stats.total_rows = 0
        stats.max_page_size = 0

        stats
      end

      attr_reader :errors
      def valid_config?
        definitions = self.class.get_config_definitions

        # Check for missing required parameters
        missing_params = definitions.select { |name, options| options[:required] && !config.key?(name) }
        if missing_params.any?
          error_messages = missing_params.map { |name, options| "Missing #{name} param - #{options[:message]}" }
          raise "#{adapter_name}Adapter: #{error_messages.join(", ")}"
        end

        # Apply default values
        definitions.each do |name, options|
          config[name] = options[:default] if options[:default] && !config.key?(name)
        end
      end
    end
  end
end
