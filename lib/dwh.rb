require 'faraday'
require 'active_support/core_ext/string/inflections'
require 'active_support/core_ext/hash/keys'
require 'active_support/core_ext/object/blank'
require 'active_support/duration'

require_relative 'dwh/version'
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
# ==== Examples
#
# Create an instance of an existing registered adapter:
#   DWH.create("snowflake", {warehouse: "wh", account_id: "myid"})
#
# Check if an adapter exists:
#   DWH.adapter?(:redshift)
#
# Register your own adatper:
#   DWH.register(:my_adapter, MyLib::MyAdapter)
module DWH
  # Top level Error class for lib.
  class DWHError < StandardError; end

  # ConfigError catches issues related to how an
  # adapter was configured and instantiated.
  class ConfigError < DWHError; end

  # ExecutionError are thrown when there is a failuire
  # to execute calls against the remote db server.
  class ExecutionError < DWHError; end

  # Connection erros are thrown when we fail to
  # obtain a connection for the target database.
  class ConnectionError < DWHError; end

  # UnspportedCapability are thrown when calling a function
  # that the target database does not support.
  class UnsupportedCapability < StandardError; end

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
