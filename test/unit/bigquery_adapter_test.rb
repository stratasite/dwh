require 'test_helper'

# Unit tests for the BigQuery adapter that do not require a live connection
# or the google-cloud-bigquery gem (it is required lazily in #connection).
# Covers dialect (settings-driven SQL functions) and BigQuery type normalisation.
class BigQueryAdapterTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:bigquery, project_id: 'my-project', dataset: 'analytics')
  end

  # --- Registration / config ---

  def test_adapter_is_registered
    assert DWH.adapter?(:bigquery)
  end

  def test_settings_loaded_from_file
    refute DWH::Adapters::BigQuery.using_base_settings?
  end

  def test_missing_dataset_raises_config_error
    assert_raises(DWH::ConfigError) { DWH.create(:bigquery, project_id: 'p') }
  end

  def test_query_timeout_default
    assert_equal 300, adapter.query_timeout
  end

  def test_inline_service_account_credentials_build_a_hash
    a = DWH.create(:bigquery, project_id: 'p', dataset: 'd', keyfile: '/ignored.json',
                              client_email: 'svc@p.iam.gserviceaccount.com', private_key: "-----BEGIN PRIVATE KEY-----\nabc")
    creds = a.send(:google_credentials)
    assert_equal 'service_account', creds['type']
    assert_equal 'p', creds['project_id']
    assert_equal 'svc@p.iam.gserviceaccount.com', creds['client_email']
    assert_match(/BEGIN PRIVATE KEY/, creds['private_key'])
  end

  def test_keyfile_path_used_when_no_inline_credentials
    a = DWH.create(:bigquery, project_id: 'p', dataset: 'd', keyfile: '/path/key.json')
    assert_equal '/path/key.json', a.send(:google_credentials)
  end

  def test_no_credentials_means_application_default
    assert_nil adapter.send(:google_credentials)
  end

  def test_close_drops_the_client_without_calling_close_on_it
    a = DWH.create(:bigquery, project_id: 'p', dataset: 'd')
    a.instance_variable_set(:@connection, Object.new) # Google::Cloud::Bigquery::Project has no #close
    a.close
    assert_nil a.instance_variable_get(:@connection)
  end

  # --- Identifier quoting ---

  def test_quote_uses_backticks
    assert_equal '`my_col`', adapter.quote('my_col')
  end

  def test_quote_replaces_characters_bigquery_rejects
    assert_equal '`Month_Post Date_`', adapter.quote('Month(Post Date)')
    assert_equal '`Ratio % - A:B`', adapter.quote('Ratio % - A:B')
    assert_equal '`strata_test.posts`', adapter.quote('strata_test.posts')
  end

  # --- Date functions ---

  def test_date_literal
    assert_equal "DATE '2024-01-15'", adapter.date_literal('2024-01-15')
  end

  def test_date_time_literal
    assert_equal "TIMESTAMP '2024-01-15 10:00:00'", adapter.date_time_literal('2024-01-15 10:00:00')
  end

  def test_truncate_date
    assert_match(/DATE_TRUNC\(created_at, month\)/, adapter.truncate_date('month', 'created_at'))
  end

  def test_truncate_week_honours_week_start_day
    assert_match(/DATE_TRUNC\(d, WEEK\(MONDAY\)\)/, adapter.truncate_date('week', 'd'))
    # Sunday is BigQuery's native week start, so no explicit WEEK(...) is needed.
    adapter.alter_settings(week_start_day: 'sunday')
    assert_match(/DATE_TRUNC\(d, week\)/, adapter.truncate_date('week', 'd'))
  ensure
    adapter.reset_settings
  end

  def test_date_add
    assert_equal 'DATE_ADD(d, INTERVAL 3 day)', adapter.date_add('day', 3, 'd')
  end

  def test_date_diff
    assert_equal 'DATE_DIFF(b, a, day)', adapter.date_diff('day', 'a', 'b')
  end

  def test_current_date
    assert_equal 'CURRENT_DATE()', adapter.current_date
  end

  def test_extract_year
    assert_equal 'EXTRACT(YEAR FROM ts)', adapter.extract_year('ts')
  end

  def test_extract_year_month
    assert_equal "CAST(FORMAT_DATE('%Y%m', ts) AS INT64)", adapter.extract_year_month('ts')
  end

  def test_extract_day_name
    assert_equal "UPPER(FORMAT_DATE('%A', ts))", adapter.extract_day_name('ts')
  end

  def test_extract_month_name_abbreviated
    assert_equal "UPPER(FORMAT_DATE('%b', ts))", adapter.extract_month_name('ts', abbreviate: true)
  end

  # --- Null handling ---

  def test_if_null
    assert_equal 'IFNULL(col, 0)', adapter.if_null('col', '0')
  end

  # --- Capabilities / keywords ---

  def test_does_not_support_temp_tables
    refute adapter.supports_temp_tables?
    assert_equal 'cte', adapter.temp_table_type
  end

  def test_reserved_and_aggregate_extras
    assert DWH::Adapters::BigQuery.reserved?('struct')
    assert DWH::Adapters::BigQuery.aggregate_function?('countif')
  end

  # --- Type normalisation ---

  def test_type_normalisation
    {
      'INT64' => 'bigint',
      'FLOAT64' => 'decimal',
      'NUMERIC' => 'decimal',
      'BIGNUMERIC' => 'decimal',
      'BYTES' => 'binary',
      'BOOL' => 'boolean',
      'DATE' => 'date',
      'DATETIME' => 'date_time',
      'TIMESTAMP' => 'date_time',
      'STRING' => 'string',
      'GEOGRAPHY' => 'string',
      'STRUCT' => 'string'
    }.each do |bq_type, expected|
      assert_equal expected, DWH::Column.new(name: 'c', data_type: bq_type).normalized_data_type, bq_type
    end
  end
end
