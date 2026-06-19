require 'test_helper'

# Unit tests for the ClickHouse adapter that do not require a live connection.
# Covers dialect (settings-driven SQL functions) and ClickHouse type normalisation.
class ClickHouseAdapterTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:clickhouse, host: 'localhost', database: 'test_db')
  end

  # --- Registration ---

  def test_adapter_is_registered
    assert DWH.adapter?(:clickhouse)
  end

  # --- Identifier quoting ---

  def test_quote_uses_backticks
    assert_equal '`my_col`', adapter.quote('my_col')
  end

  def test_string_lit
    assert_equal "'hello world'", adapter.string_lit('hello world')
  end

  # --- Date functions ---

  def test_truncate_date
    # truncate_date wraps non-timestamp units in CAST(... AS DATE) — base behaviour.
    # ClickHouse CAST syntax matches the base so this is valid ClickHouse SQL.
    result = adapter.truncate_date('month', 'created_at')
    assert_match(/date_trunc\('month', created_at\)/, result)
  end

  def test_date_literal
    assert_equal "toDate('2024-01-15')", adapter.date_literal('2024-01-15')
  end

  def test_date_time_literal
    assert_equal "toDateTime('2024-01-15 10:00:00')", adapter.date_time_literal('2024-01-15 10:00:00')
  end

  def test_current_date
    assert_equal 'today()', adapter.current_date
  end

  def test_current_timestamp
    assert_equal 'now()', adapter.current_timestamp
  end

  def test_extract_year
    assert_equal 'toYear(ts)', adapter.extract_year('ts')
  end

  def test_extract_month
    assert_equal 'toMonth(ts)', adapter.extract_month('ts')
  end

  def test_extract_day_of_week
    assert_equal 'toDayOfWeek(ts)', adapter.extract_day_of_week('ts')
  end

  # --- Null handling ---

  def test_if_null
    assert_equal 'ifNull(col, 0)', adapter.if_null('col', '0')
  end

  def test_null_if
    assert_equal 'nullIf(col, 0)', adapter.null_if('col', '0')
  end

  # --- Capabilities ---

  def test_supports_cte
    assert adapter.supports_common_table_expressions?
  end

  def test_does_not_support_temp_tables
    refute adapter.supports_temp_tables?
  end

  def test_temp_table_type_is_cte
    assert_equal 'cte', adapter.temp_table_type
  end

  def test_supports_window_functions
    assert adapter.supports_window_functions?
  end

  # --- Type normalisation ---

  def test_uint8_is_integer
    assert_equal 'integer', col('UInt8').normalized_data_type
  end

  def test_uint32_is_integer
    assert_equal 'integer', col('UInt32').normalized_data_type
  end

  def test_uint64_is_bigint
    assert_equal 'bigint', col('UInt64').normalized_data_type
  end

  def test_int32_is_integer
    assert_equal 'integer', col('Int32').normalized_data_type
  end

  def test_int64_is_bigint
    assert_equal 'bigint', col('Int64').normalized_data_type
  end

  def test_float64_is_decimal
    assert_equal 'decimal', col('Float64').normalized_data_type
  end

  def test_decimal_type
    assert_equal 'decimal', col('Decimal(18, 4)').normalized_data_type
  end

  def test_string_type
    assert_equal 'string', col('String').normalized_data_type
  end

  def test_fixedstring_is_string
    assert_equal 'string', col('FixedString(36)').normalized_data_type
  end

  def test_date_type
    assert_equal 'date', col('Date').normalized_data_type
  end

  def test_date32_is_date
    assert_equal 'date', col('Date32').normalized_data_type
  end

  def test_datetime_type
    assert_equal 'date_time', col('DateTime').normalized_data_type
  end

  def test_datetime64_is_date_time
    assert_equal 'date_time', col('DateTime64(3)').normalized_data_type
  end

  def test_bool_type
    assert_equal 'boolean', col('Bool').normalized_data_type
  end

  def test_nullable_unwrap
    assert_equal 'integer', col('Nullable(UInt32)').normalized_data_type
    assert_equal 'string',  col('Nullable(String)').normalized_data_type
    assert_equal 'date',    col('Nullable(Date)').normalized_data_type
  end

  def test_lowcardinality_unwrap
    assert_equal 'string', col('LowCardinality(String)').normalized_data_type
    assert_equal 'integer', col('LowCardinality(UInt32)').normalized_data_type
  end

  def test_uuid_is_string
    assert_equal 'string', col('UUID').normalized_data_type
  end

  private

  def col(type)
    DWH::Column.new(name: 'test', data_type: type)
  end
end
