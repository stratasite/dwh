require 'csv'
require_relative 'settings'
require_relative 'capabilities'
require_relative 'functions'
require_relative 'behaviors'
require_relative 'logger'

module DWH
  # Encapsulates functionality related to all Adapter implementations
  module Adapters
    class Boolean; end

    # @abstract Adapters base class. All adapters should inherit from this class
    #   and implement these core required methods:
    #
    #   * {#stats} - get table statistics
    #   * {#tables} - list tables
    #   * {#metadata} - get metadata for a specific table
    #   * {#execute} - run query and return result
    #   * {#execute_stream} - run query and stream results to provided IO
    #   * {#stream} - run query and yeild streaming results
    #
    # Adapter implementations can declare configuration options,
    # defaults, and whether it is required. This is a class
    # level method. They will be validated and a {ConfigError} will be raised
    # if there is an issue. Methods not implemented will raise {NotImplementedError}
    #
    # Additionally, if certain setting need to be overridden you can add a settings
    # file in a relative directory like so: *settings/my_adapter.yml*. Or, you can specify
    # an exact settings file location at the class level:
    #
    #   class MyAdapter < DWH::Adapters::Adapter
    #     settings_file_path "my_dir/my_settings.yml"
    #   end
    #
    # @example
    #   class MyAdapter < DWH::Adapters::Adapter
    #     config :username, String, required: true, message: "login id of the current user"
    #     config :port, Integer, required: true, default: 5432
    #   end
    class Adapter
      extend Settings
      include Capabilities
      include Functions
      include Behaviors
      include Logger

      # Define the configurations required for the adapter to
      # connect and query target database.
      #
      # @param name [String, Symbol] name of the configuration
      # @param type [Constant] ruby type of the configuration
      # @param options [Hash] options for the config
      # @option options [Boolean] :required Whether option is required
      # @option options [*] :default The default value
      # @option options [String] :message The error message or info displayed
      # @return [Hash]
      def self.config(name, type, options = {})
        configuration[name.to_sym] = {
          type: type,
          required: options[:required] || false,
          default: options[:default],
          message: options[:message] || "Invalid or missing parameter: #{name}",
          allowed: options[:allowed] || []
        }

        define_method(name.to_sym) do
          config[name.to_sym]
        end
      end

      # Get the adapter class level configuration settings
      # @return [Hash]
      def self.configuration
        @configuration ||= {}
      end

      # Instance level configurations
      # @return [Hash] the actual instance configuration
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
      # determines how we connect.
      # @return [Hash] symbolized hash of settings
      attr_reader :settings

      # Allows an already instantiated adapter to change its current settings.
      # this might be useful in situations where behavior needs to be modified
      # on runtime basis.
      # @return [Hash] the complete settings with changes merged
      def alter_settings(changes = {})
        reset_settings unless @original_settings.nil?
        @original_settings = @settings
        @settings.merge!(changes)
      end

      # This returns settings back to its original state prior to
      # running alter_settings.
      # @return [Hash] with original settings
      def reset_settings
        @settings = @original_settings if @original_settings
      end

      # Creates a connection to the target database and returns the
      # connection object or self
      def connection
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Tests the connection to the target database and returns true
      # if successful, or raise Exception or false
      # connection object or self
      # @return [Boolean]
      # @raise [ConnectionError] when a connection cannot be made
      def test_connection(raise_exception: false)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Test connection and raise exception if connection
      # fails.
      # @return [Boolean]
      # @raise [ConnectionError]
      def connect!
        test_connection(raise_exception: true)
      end

      # Tests whether the dtabase can be connected
      # @return [Boolean]
      def connect?
        test_connection(raise_exception: false)
      end

      # Close the connection if it was created.
      def close
        @connection&.close
      end

      # Execute sql on the target database.
      #
      # @param sql [String] actual sql
      # @param format [Symbol, String] return format type
      #   - array returns array of array
      #   - object returns array of Hashes
      #   - csv returns as csv
      #   - native returns the native result from any clients used
      #     - For example: Postgres using pg client will return PG::Result
      #     - Http clients will returns the HTTP response object
      # @param retries [Integer] number of retries in case of failure. Default is 0
      # @return [Array<Array>,Hash, CSV, Native]
      # @raise [ConnectionError, ExecutionError]
      def execute(sql, format: :array, retries: 0)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Execute sql and stream responses back. Data is writtent out in CSV format
      # to the provided IO object.
      #
      # @param sql [String] actual sql
      # @param io [IO] IO object to write records to
      # @param stats [StreamingStats] collect stats and preview data this is optional
      # @param retries [Integer] number of retries in case of failure
      # @return [IO]
      # @raise [ConnectionError, ExecutionError]
      def execute_stream(sql, io, stats: nil, retries: 0)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Executes the given sql and yields the streamed results
      # to the given block.
      #
      # @param sql [String] actual sql
      # @param retries [Integer] number of retries in case of failure. Default is 0
      # @yield [chunk] Yields a streamed chunk as it streams in. The chunk type
      #   might vary depending on the target db and settings
      # @raise [ConnectionError, ExecutionError]
      def stream(sql, retries: 0, &block)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Returns basic stats of a given table.  Will typically include row_count,
      # date_start, and date_end.
      #
      # @param table [String] table name
      # @param catalog [String] optional catalog or equivalent name space.
      #   will be ignored if the adapter doesn't support
      # @param schema [String] optional schema to scope to.
      #   will be ignored if the adapter doesn't support
      # @return [DWH::Table]
      # @raise [ConnectionError, ExecutionError]
      #
      # @example
      #   stats("public.big_table", date_column: "fact_date")
      #   stats("big_table")
      #   stats("big_table",schema: "public")
      def stats(table, date_column: nil, **qualifiers)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Get all tables available in the
      # target db.  It will use the default catalog and schema
      # config only specified here.
      #
      # @param catalog [String] optional catalog or equivalent name space.
      #   will be ignored if the adapter doesn't support
      # @param schema [String] optional schema to scope to.
      #   will be ignored if the adapter doesn't support
      # @return [Array<String>]
      def tables(**qualifiers)
        raise NotImplementedError, "#{self.class} has not implemented method '#{__method__}'"
      end

      # Check if table exists in remote db.
      #
      # @param catalog [String] optional catalog or equivalent name space.
      #   will be ignored if the adapter doesn't support
      # @param schema [String] optional schema to scope to.
      #   will be ignored if the adapter doesn't support
      # @return [Boolean]
      def table?(table, **qualifiers)
        tables(**qualifiers).include?(table)
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

      # Will call the block with retries given by the
      # max attempts param. If max attempts is 0, it
      # will just return the block.call
      #
      # @param max_attempts [Integer] max number of retries
      def with_retry(max_attempts = 2, &block)
        return block.call if max_attempts.zero?

        attempts = 0

        begin
          attempts += 1
          block.call
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

      # Wraps an SQL execution with debug logging.
      # It sill include execution time.
      #
      # @param sql [String] actual sql being executed
      # @return execution results (see #execute)
      def with_debug(sql, &block)
        starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        logger.debug("=== SQL === \n#{sql}")

        result = block.call

        ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        elapsed = ending - starting
        logger.debug("=== FINISHED SQL (#{elapsed.round(1)} secs) ===")

        result
      end

      # Adapter name from the class name
      # @return [String]
      def adapter_name
        self.class.name.demodulize
      end

      # If any extra connection params were passed in the config
      # object, this will return it.
      #
      # @return [Hash] default empty hash
      def extra_connection_params
        config[:extra_connection_params] || {}
      end

      # If the adapter supports it, will pass on extra query params
      # from the config to the executor.
      #
      # @return [Hash] default empty hash
      def extra_query_params
        config[:extra_query_params] || {}
      end

      protected

      # Checks if the required configurations and type is passed
      # when the adapter is initialized.
      def valid_config?
        definitions = self.class.configuration

        # Check for missing required parameters
        missing_params = definitions.select do |name, options|
          options[:required] && !config.key?(name) && options[:default].nil?
        end

        if missing_params.any?
          error_messages = missing_params.map { |name, options| "Missing #{name} param - #{options[:message]}" }
          raise ConfigError, "#{adapter_name} Adapter: #{error_messages.join(', ')}"
        end

        definitions.each do |name, opts|
          unless opts[:type].is_a?(Class)
            raise ConfigError,
                  "Adapter is not defined properly. Uknown configuration type #{opts[:type]} for #{name}. Should be a class like String, Integer etc."
          end

          raise ConfigError, "Invalid value. Only allowed: #{opts[:allowed]}." if opts[:allowed].any? && !opts[:allowed].include?(config[name])

          config[name] = opts[:default] if opts[:default] && !config.key?(name)

          if opts[:required] && !config[name].is_a?(opts[:type]) && !opts[:type].is_a?(Boolean)
            raise ConfigError, "#{name} should be a #{opts[:type]}. Got #{opts[name.to_sym].class.name}"
          end
        end
      end
    end
  end
end
