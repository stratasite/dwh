require 'faraday'
require 'active_support/core_ext/string/inflections'
require 'active_support/core_ext/hash/keys'
require 'active_support/core_ext/object/blank'
require 'active_support/duration'

require_relative 'dwh/version'
require_relative 'dwh/errors'
require_relative 'dwh/logger'
require_relative 'dwh/streaming_stats'
require_relative 'dwh/factory'
require_relative 'dwh/adapters'
require_relative 'dwh/table'
require_relative 'dwh/table_stats'
require_relative 'dwh/adapters/druid'
require_relative 'dwh/adapters/trino'
require_relative 'dwh/adapters/postgres'
require_relative 'dwh/adapters/snowflake'
require_relative 'dwh/adapters/my_sql'
require_relative 'dwh/adapters/sql_server'
require_relative 'dwh/adapters/duck_db'
require_relative 'dwh/adapters/athena'

# DWH encapsulates the full functionality of this gem.
#
# @example Create an instance of an existing registered adapter:
#   DWH.create("snowflake", {warehouse: "wh", account_id: "myid"})
#
# @example Check if an adapter exists:
#   DWH.adapter?(:redshift)
#
# @example Register your own adatper:
#   DWH.register(:my_adapter, MyLib::MyAdapter)
module DWH
  INT_TYPES = %w[int integer bigint tinyint smallint].freeze
  DEC_TYPES = %w[real float double decimal].freeze
  STRING_TYPES = %w[string char varchar varbinary json].freeze
  TIMESTAMP_TYPES = ['timestamp with time zone', 'timestamp(p)', 'timestamp'].freeze
  DATE_TYPES = %w[date].freeze

  extend Factory

  # Register default adapters
  register(:druid, Adapters::Druid)
  register(:postgres, Adapters::Postgres)
  register(:trino, Adapters::Trino)
  register(:snowflake, Adapters::Snowflake)
  register(:mysql, Adapters::MySql)
  register(:sqlserver, Adapters::SqlServer)
  register(:duckdb, Adapters::DuckDb)
  register(:athena, Adapters::Athena)

  # start_reaper
end
