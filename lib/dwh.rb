# frozen_string_literal: true
require "faraday"
require 'active_support/core_ext/string/inflections'
require 'active_support/core_ext/hash/keys'
require "active_support/core_ext/object/blank"

require_relative "dwh/version"
require_relative "dwh/logger"
require_relative "dwh/factory"
require_relative "dwh/adapters"
require_relative "dwh/table"
require_relative "dwh/table_stats"
require_relative "dwh/adapters/druid"
require_relative "dwh/adapters/trino"
require_relative "dwh/adapters/postgres"
require_relative "dwh/adapters/snowflake"
require_relative "dwh/adapters/my_sql"

module DWH
  # ConfigError catches issues related to how an
  # adapter was configured and instantiated.
  class ConfigError < StandardError; end
  # ExecutionError are thrown when there is a failuire
  # to execute calls against the remote db server.
  class ExecutionError < StandardError; end

  # UnspportedCapability are thrown when calling a function
  # that the target database does not support.
  class UnsupportedCapability < StandardError; end
  
  INT_TYPES = %w[int integer bigint tinyint smallint]
  DEC_TYPES = %w[real float double decimal]
  STRING_TYPES = %w[string char varchar varbinary json]
  TIMESTAMP_TYPES = ["timestamp with time zone", "timestamp(p)", "timestamp"]
  DATE_TYPES = %w[date]

  extend Factory

  # Register default adapters
  register(:druid, Adapters::Druid)
  register(:postgres, Adapters::Postgres)
  register(:trino, Adapters::Trino)
  register(:snowflake, Adapters::Snowflake)
  register(:mysql, Adapters::MySql)

  # start_reaper
end
