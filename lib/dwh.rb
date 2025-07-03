# frozen_string_literal: true

require_relative "dwh/version"
require_relative "dwh/logger"
require_relative "dwh/functions"
require_relative "dwh/capabilities"
require_relative "dwh/settings"
require_relative "dwh/behaviors"
require_relative "dwh/base"
require_relative "dwh/factory"
require_relative "dwh/druid"
require_relative "dwh/trino"
require_relative "dwh/postgres"
require_relative "dwh/snowflake"
require_relative "dwh/my_sql"


module DWH
  class Error < StandardError; end

  INT_TYPES       = %w[int integer bigint tinyint smallint]
  DEC_TYPES       = %w[real float double decimal]
  STRING_TYPES    = %w[string char varchar varbinary json]
  TIMESTAMP_TYPES = [ "timestamp with time zone", "timestamp(p)", "timestamp" ]
  DATE_TYPES      = %w[date]

  extend Factory

  # Register default adapters
  register(:druid, Druid)
  register(:postgres, Postgres)
  register(:trino, Trino)
  register(:snowflake, Snowflake)
  register(:mysql, MySql)

  # start_reaper
end
