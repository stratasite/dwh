require 'yaml'

module DWH
  # Functions related to loading and managing the Adapters
  # settings. These settings should by default have the same name
  # as the adapter but lower case and no camelcase.  i.e. MySql adapter
  # has a settings file called mysql.yml.
  #
  # When creating a new adapter copy {settings/base.yml} and modify it
  # to suit your adapters needs.
  #
  # An adapters settings are merged into the base settings config. So each adapter
  # doesn't have to define every property when they are the same.
  #
  # By default the file will be looked for in relative location like settings/myadapter.yml.
  # However, you can specify the locastion with (@see #settings_file_path)
  module Settings
    include Logger

    # Gets the current loaded adatper settings. If nil, then
    # load_settings hasn't been called.
    attr_reader :adapter_settings

    # This is the default base settings that each adapter can override
    # with its own yaml files.
    BASE_SETTINGS_FILE = File.join(__dir__, 'settings', 'base.yml')

    # This will load adapter level settings. These settings can be
    # overridden at runtime by calling alter_settings after
    # an adapter is initialized.
    def load_settings
      return unless @adapter_settings.nil?

      logger.debug "+++ LOADING SETTINGS: #{name} +++"

      @using_base = true
      @adapter_settings = YAML.load_file(BASE_SETTINGS_FILE)

      if File.exist?(settings_file)
        @using_base = false
        settings_from_file = YAML.load_file(settings_file) || {}
        @adapter_settings.merge!(settings_from_file)
      else
        logger.debug "#{adapter_name} Adapter didn't have a settings YAML file. Using only base settings."
      end

      @adapter_settings.transform_keys! do |key|
        key.to_sym
      rescue StandardError
        key
      end
    end

    # By default settings_file are expected to be in a
    # relative directory called settings. If not,
    # change the settings file with call to settings_file_path FILE_PATH
    def settings_file
      @settings_file ||= File.join(__dir__, 'settings', "#{adapter_name}.yml")
    end

    # Allows the manual configuration of where to
    # load default database settings from.
    #
    # It will reload settings if adapter settings has already
    # been loaded.
    # @param [String] file - path or file name string
    def settings_file_path(file)
      @settings_file = file
      return if @adapter_settings.nil?

      @adapter_settings = nil
      load_settings
    end

    def adapter_name
      name.split('::').last.downcase
    end

    def using_base_settings?
      @using_base
    end

    # Returns the full reserved-keyword list for this adapter class
    # (baseline from base.yml merged with any extra_reserved_keywords from the
    # adapter's own settings file). Safe to call at class level without an instance.
    def reserved_keywords
      base  = Array(adapter_settings[:reserved_keywords])
      extra = Array(adapter_settings[:extra_reserved_keywords])
      (base + extra).map { |k| k.to_s.downcase }.uniq.freeze
    end

    # Returns the full aggregate-function list for this adapter class
    # (baseline from base.yml merged with any extra_aggregate_functions from the
    # adapter's own settings file). Safe to call at class level without an instance.
    def aggregate_functions
      base  = Array(adapter_settings[:aggregate_functions])
      extra = Array(adapter_settings[:extra_aggregate_functions])
      (base + extra).map { |k| k.to_s.downcase }.uniq.freeze
    end

    def reserved?(name)
      reserved_keywords.include?(name.to_s.downcase)
    end

    def aggregate_function?(name)
      aggregate_functions.include?(name.to_s.downcase)
    end
  end
end
