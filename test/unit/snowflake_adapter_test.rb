# frozen_string_literal: true

require 'test_helper'

# Unit tests for the Snowflake adapter that do not require a live connection.
# The metadata lookups are built as SQL strings, so a capturing subclass is
# enough to assert what they ask information_schema for.
class SnowflakeAdapterTest < Minitest::Test
  class CapturingSnowflake < DWH::Adapters::Snowflake
    attr_reader :last_sql

    def valid_config? = true

    def execute(sql, **)
      @last_sql = sql
      []
    end
  end

  def setup
    DWH.register(:capturing_snowflake, CapturingSnowflake)
  end

  def teardown
    DWH.unregister(:capturing_snowflake)
  end

  def adapter(**overrides)
    DWH.create(:capturing_snowflake, auth_mode: 'pat', account_identifier: 'org-acct',
                                     personal_access_token: 'x', database: 'SAMPLE_DATA',
                                     **overrides)
  end

  # SNOWFLAKE_SAMPLE_DATA carries the same table in several schemas, so a listing
  # that ignores the configured one repeats every name once per schema.
  def test_tables_scopes_to_the_configured_schema
    a = adapter(schema: 'TPCDS_SF10TCL')
    a.tables
    assert_includes a.last_sql, "table_schema = 'TPCDS_SF10TCL'"
  end

  def test_tables_lets_an_explicit_qualifier_win
    a = adapter(schema: 'TPCDS_SF10TCL')
    a.tables(schema: 'TPCH_SF1')
    assert_includes a.last_sql, "table_schema = 'TPCH_SF1'"
    refute_includes a.last_sql, 'TPCDS_SF10TCL'
  end

  def test_tables_without_a_configured_schema_is_unscoped
    a = adapter
    a.tables
    refute_includes a.last_sql, 'table_schema'
  end

  # Same omission made every column come back once per schema, which builds a
  # table with each column duplicated.
  def test_metadata_scopes_to_the_configured_schema
    a = adapter(schema: 'TPCDS_SF10TCL')
    a.metadata('item')
    assert_includes a.last_sql, "table_schema = 'TPCDS_SF10TCL'"
  end

  # A table created quoted keeps its case, so forcing the needle to upper found
  # nothing and reported a table with no columns rather than an error.
  def test_metadata_matches_the_table_name_without_case
    a = adapter(schema: 'PUBLIC')
    a.metadata('lower_item')
    assert_includes a.last_sql, "UPPER(table_name) = UPPER('lower_item')"
  end
end
