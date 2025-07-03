module Adapters
  module Db
    module Logger
      extend ActiveSupport::Concern

      def logger
        return @logger if @logger

        if defined?(Rails)
          @logger = Rails.logger
        else
          @logger = Logger.new(STDOUT)
        end

        @logger
      end

      class_methods do

        def logger
          return @logger if @logger

          if defined?(Rails)
            @logger = Rails.logger
          else
            @logger = Logger.new(STDOUT)
          end

          @logger
        end

      end
    end
  end
end