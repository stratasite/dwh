module Adapters
    module Db
        module Settings
            include Logger

            attr_accessor :settings_file, :adapter_settings
            BASE_SETTINGS_FILE = File.join(__dir__, "settings", "base.yml")


            def load_settings
                return unless @adapter_settings.nil?

                logger.debug "+++ LOADING SETTINGS: #{self.name.demodulize} +++"

                @adapter_settings = YAML.load_file(BASE_SETTINGS_FILE)

                if File.exist?(settings_file)
                    adapter_settings = YAML.load_file(settings_file) || {}
                    @adapter_settings.merge!(adapter_settings)
                else
                    logger.warn "#{adapter_name} Adapter didn't have a settings YAML file. Using only base settings."
                end

                @adapter_settings.symbolize_keys!
            end

            def settings_file
                @settings_file ||= File.join(__dir__, "settings", "#{adapter_name}.yml")
            end

            def adapter_name
                self.name.demodulize.downcase
            end
        end
    end
end
