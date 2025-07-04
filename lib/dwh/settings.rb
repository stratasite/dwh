require "yaml"
module DWH
  module Settings
    include Logger

    attr_writer :settings_file
    attr_accessor :adapter_settings
    BASE_SETTINGS_FILE = File.join(__dir__, "settings", "base.yml")

    def load_settings
      return unless @adapter_settings.nil?

      logger.debug "+++ LOADING SETTINGS: #{name} +++"

      @adapter_settings = YAML.load_file(BASE_SETTINGS_FILE)

      if File.exist?(settings_file)
        adapter_settings = YAML.load_file(settings_file) || {}
        @adapter_settings.merge!(adapter_settings)
      else
        logger.warn "#{adapter_name} Adapter didn't have a settings YAML file. Using only base settings."
      end

      @adapter_settings.transform_keys(&:to_sym)
    end

    def settings_file
      @settings_file ||= File.join(__dir__, "settings", "#{adapter_name}.yml")
    end

    def adapter_name
      name.split("::").last.downcase
    end
  end
end
