require "logger"

module DWH
  module Logger
    def logger
      DWH::Logger.logger
    end

    class << self
      def logger
        @logger ||= create_logger
      end

      private

      def create_logger
        if rails_logger_available?
          Rails.logger
        else
          standalone_logger
        end
      end

      def rails_logger_available?
        defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger
      end

      def standalone_logger
        logger = ::Logger.new($stdout)
        logger.level = log_level
        logger.formatter = proc do |severity, datetime, _progname, msg|
          "[#{datetime.strftime("%Y-%m-%d %H:%M:%S")}] #{severity} DWH: #{msg}\n"
        end
        logger
      end

      def log_level
        case ENV["DWH_LOG_LEVEL"]&.downcase
        when "debug" then ::Logger::DEBUG
        when "info" then ::Logger::INFO
        when "warn" then ::Logger::WARN
        when "error" then ::Logger::ERROR
        when "fatal" then ::Logger::FATAL
        else ::Logger::INFO
        end
      end
    end
  end
end
