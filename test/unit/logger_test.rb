# frozen_string_literal: true

require "test_helper"

class TestLogger < Minitest::Test
  def setup
    # Reset logger instance between tests
    DWH::Logger.instance_variable_set(:@logger, nil)
    ENV.delete("DWH_LOG_LEVEL")
  end

  def teardown
    # Clean up after tests
    DWH::Logger.instance_variable_set(:@logger, nil)
    ENV.delete("DWH_LOG_LEVEL")
    Object.send(:remove_const, :Rails) if defined?(Rails)
  end

  def test_standalone_logger_creation
    logger = DWH::Logger.logger

    assert_instance_of Logger, logger
    assert_equal Logger::INFO, logger.level
  end

  def test_rails_logger_when_available
    # Mock Rails with logger
    rails_logger = Logger.new(StringIO.new)
    rails_mock = Class.new do
      define_singleton_method(:logger) { rails_logger }
    end

    Object.const_set(:Rails, rails_mock)

    logger = DWH::Logger.logger

    assert_equal rails_logger, logger
  end

  def test_log_level_configuration
    ENV["DWH_LOG_LEVEL"] = "debug"

    logger = DWH::Logger.logger

    assert_equal Logger::DEBUG, logger.level
  end

  def test_log_level_configuration_with_invalid_level
    ENV["DWH_LOG_LEVEL"] = "invalid"

    logger = DWH::Logger.logger

    assert_equal Logger::INFO, logger.level
  end

  def test_instance_method_delegates_to_class
    test_class = Class.new do
      include DWH::Logger
    end

    instance = test_class.new

    assert_equal DWH::Logger.logger, instance.logger
  end

  def test_logger_singleton_behavior
    logger1 = DWH::Logger.logger
    logger2 = DWH::Logger.logger

    assert_equal logger1, logger2
  end

  def test_standalone_logger_format
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::INFO
    logger.formatter = proc do |severity, datetime, _progname, msg|
      "[#{datetime.strftime("%Y-%m-%d %H:%M:%S")}] #{severity} DWH: #{msg}\n"
    end

    logger.info("test message")
    output.rewind
    log_output = output.read

    assert_match(/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] INFO DWH: test message/, log_output)
  end
end
