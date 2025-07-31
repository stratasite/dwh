require 'csv'
require_relative 'settings'
require_relative 'capabilities'
require_relative 'functions'
require_relative 'behaviors'
require_relative 'logger'

module DWH
  module Adapters
    # Adapters base class. All adapters should inherit from this class
    # and implement these core required methods:
    #
    # stats
    # tables
    # metadata
    # execute
    # execute_stream
    # stream
    #
    # Adapter implementations can declare configuration options,
    # defaults, and whether it is required. This is a class
    # level method. This will be checked against and ConfigError will be raised
    # if there is an issue.
    #
    # ==== Examples
    #
    # class MyAdapter < DWH::Adapters::Adapter
    #   define_config :username, required: true, message: "login id of the current user"
    #   define_config :port, required: true, default: 5432
    # end
    #
    # Additionally, if certain setting need to be overridden you can add a settings
    # file in a relative directory like so: settings/my_adapter.yml
    #
    # Alternatively, you can specify and exact settings file location at the class level:
    # class MyAdapter < DWH::Adapters::Adapter
    #   settings_file_path "my_dir/my_settings.yml"
    # end
    #
    # ==== Config Parameters
    #
    # These are the required or optional configuration options when connecting to
    # or querying the target db:
    #
    #   *name*: The name of the configuration option
    #   *required*: Whether or not its required. Will throw error if required is not present
    #   *default*: default value when missing
    #   *message*: Error message when missing
    class Adapter
      extend Settings
      include Capabilities
      include Functions
      include Behaviors
      include Logger

      def self.define_config(name, options = {})
        config_definitions[name.to_sym] = {
          required: options[:required] || false,
          default: options[:default],
          message: options[:message] || "Invalid or missing parameter: #{name}"
        }
      end

      def self.config_definitions
        @config_definitions ||= {}
      end

      # Connection configuration information as setup by
      # define_config method calls.
      attr_reader :config

      def initialize(config)
        @config = config.symbolize_keys
        # Per instance customization of general settings
        # So you can have multiple connections to Trino
        # but exhibit diff behavior
        @settings = self.class.adapter_settings.merge(
          (config[:settings] || {}).symbolize_keys
        )

        valid_config?
      end

      # This is the actual runtime settings used by the adapter
      # once initialized. During intialization settings could be
      # overridden. Settings are different from configuration in that
      # settings control behaviour and syntax while configuration
      # determins how we connect.
      attr_reader :settings

      # Allows an already instantiated adapter to change its current settings.
      # this might be useful in a connection pool situation.
      def alter_settings(changes = {})
        reset_settings unless @original_settings.nil?
        @original_settings = @settings
        @settings.merge!(changes)
      end

      # This returns settings back to its original state prior to
      # running alter_settings.
      def reset_settings
        @settings = @original_settings if @original_settings
      end

      def connection
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      def close
        @connection&.close
      end

      # Execute sql on db.
      #
      # @param sql [String] actual sql
      # @param format [String]
      #   - array returns array of array
      #   - object returns array of Hashes
      #   - csv returns string row generated as csv row
      #   - native returns the native result from any clients used
      #     - For example: Postgres using pg client will return PG::Result
      # @param retries [Integer] number of retries in case of failure. Default is 0
      # @return [Array[]] | [Hash] | Native
      def execute(sql, format: 'array', retries: 0)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Execute sql and stream responses back. Data is writtent out in CSV format
      # to the provided IO object.
      #
      # @param sql [String] - actual sql
      # @param io [IO] - IO object to write records to
      # @param stats [DWH::StreamingStats] - collect stats and preview data this is optional
      # @param retries [Integer] number of retries in case of failure. Default is 0
      # @return [IO]
      def execute_stream(sql, io, stats: nil, retries: 0)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Executes the given sql and yields the streamed results
      # to the given block.
      def stream(sql, retries: 0, &block)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Returns basic stats of a given table.  Will typically include row_count,
      # date_start, and date_end.
      #
      # In druid or tables where we know the partitioning date column we will
      # find the min max dates along with date range.
      #
      # Example:
      #   stats("public.big_table", date_column: "fact_date")
      #   stats("big_table")
      #   stats("big_table",schema: "public")
      #
      # @param table [String] - table name
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [DWH::TableStats]
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
      def table?(table, catalog: nil, schema: nil)
        tables(catalog: catalog, schema: schema).include?(table)
      end

      # Get the schema structure of a given a given table_name.
      # Pass in optional catalog and schema info.
      #
      # Example:
      #   metadata("public.big_table")
      #   metadata("big_table")
      #   metadata("big_table",schema: "public")
      #
      # @param table [String] - table name
      # @param catalog [String] - optional catalog or equivalent name space
      # @param schema [String] - optional schema to scope to.
      # @return [DWH::Table]
      def metadata(table, catalog: nil, schema: nil)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      def with_retry(max_attempts = 2)
        # In case a retry isn't needed but is wrapped by default
        # somewhere.
        return yield if max_attempts.zero?

        attempts = 0

        begin
          attempts += 1
          yield
        rescue StandardError => e
          if attempts < max_attempts
            logger.warn "Attempt #{attempts} failed with error: #{e.message}. Retrying..."
            retry
          else
            logger.error "Failed after #{attempts} attempts with error: #{e.message}"
            raise
          end
        end
      end

      def with_debug(sql)
        starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        logger.debug("=== SQL === \n#{sql}")

        result = yield

        ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        elapsed = ending - starting
        logger.debug("=== FINISHED SQL (#{elapsed.round(1)} secs) ===")

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

      def valid_config?
        definitions = self.class.config_definitions

        # Check for missing required parameters
        missing_params = definitions.select do |name, options|
          options[:required] && !config.key?(name) && options[:default].nil?
        end
        if missing_params.any?
          error_messages = missing_params.map { |name, options| "Missing #{name} param - #{options[:message]}" }
          raise ConfigError, "#{adapter_name} Adapter: #{error_messages.join(', ')}"
        end

        # Apply default values
        definitions.each do |name, options|
          config[name] = options[:default] if options[:default] && !config.key?(name)
        end
      end
    end
  end
end
