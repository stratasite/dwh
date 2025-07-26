require "yaml"
module DWH
  module Settings
    include Logger

    # Gets the current loaded adatper settings. If nil, then
    # load_settings hasn't been called.
    attr_reader :adapter_settings

    # This is the default base settings that each adapter can override
    # with its own yaml files.
    BASE_SETTINGS_FILE = File.join(__dir__, "settings", "base.yml")

    # This will load adapter level settings. These settings can be
    # overridden at runtime by calling alter_settings after
    # an adapter is initialized.
    def load_settings
      return unless @adapter_settings.nil?
      logger.debug "+++ LOADING SETTINGS: #{name} +++"

      @using_base = true
      @adapter_settings = YAML.load_file(BASE_SETTINGS_FILE)

      if File.exist?(settings_file)
        @using_base = true
        settings_from_file = YAML.load_file(settings_file) || {}
        @adapter_settings.merge!(settings_from_file)
      else
        logger.debug "#{adapter_name} Adapter didn't have a settings YAML file. Using only base settings."
      end

      @adapter_settings.symbolize_keys!
    end

    def settings_file
      @settings_file ||= File.join(__dir__, "settings", "#{adapter_name}.yml")
    end
 
    # Allows the manual configuration of where to 
    # load default database settings from.
    #
    # It will reload settings if adapter settings has already
    # been loaded.
    def settings_file=(file)
      @settings_file = file
      if !@adapter_settings.nil?
        @adapter_settings = nil
        load_settings
      end
    end

    def adapter_name
      self.name.demodulize.downcase
    end

    def using_base_settings?
      @using_base
    end
  end
end

