# frozen_string_literal: true

require_relative "dwh/version"
require_relative "dwh/logger"
require_relative "dwh/factory"
require_relative "dwh/adapters"
require_relative "dwh/adapters/druid"
require_relative "dwh/adapters/trino"
require_relative "dwh/adapters/postgres"
require_relative "dwh/adapters/snowflake"
require_relative "dwh/adapters/my_sql"

module DWH
  class Error < StandardError; end

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
