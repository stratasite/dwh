require 'test_helper'

class CloudSnowflakeTest < Minitest::Test
  def adapter
    @adapter ||=
      DWH.create(:snowflake,
                 { database: 'TEST_DB', auth_mode: 'pat',
                   account_identifier: 'MYACCOUNT',
                   personal_access_token: '' })
  end

  def test_basic_connection
    assert adapter.connect?
  end

  def test_get_table_list
    res = adapter.tables(schema: 'public')
    assert_equal 2, res.size
  end

  def test_get_table_stats
    stats = adapter.stats('posts', date_column: 'created_at')
    assert_equal 4, stats.row_count
    assert stats.date_start.is_a?(Date)
    assert stats.date_end.is_a?(Date)
  end

  def test_can_get_metadata
    md = adapter.metadata('posts')
    assert_equal 7, md.columns.size
    col = md.find_column('created_at')
    assert col, 'column should be found'
    assert_equal 'date_time', col.normalized_data_type
    boolcol = md.find_column('published')
    assert_equal 'boolean', boolcol.normalized_data_type
  end

  def test_get_tables_from_other_schema
    tables = adapter.tables(schema: 'information_schema')
    assert tables.include?('APPLICABLE_ROLES')
  end

  def test_get_md_other_schema_table
    md = adapter.metadata('tables', schema: 'information_schema')
    assert_equal 29, md.columns.size
  end

  def test_execute_basic
    res = adapter.execute('select 1')
    assert_equal 1, res[0][0].to_i
  end

  def test_execute_bad_sql
    assert_raises DWH::ExecutionError do
      adapter.execute('select safsdasdf')
    end

    assert_raises DWH::ExecutionError do
      adapter.execute('select * from table_does_not_exist')
    end
  end

  def test_execute_formats
    %i[array object csv native].each do |format|
      res = adapter.execute('select * from users', format: format)
      case format
      when :array
        assert_equal Array, res[0].class
      when :object
        assert_equal Hash, res[0].class
      when :csv
        assert_match(/Jane\sSmith/, res)
      else
        assert_equal Hash, res.class
      end
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream 'select * from users', io, stats: stats
    # first line is headers
    assert_equal 4, res.each_line.count
    assert_equal 3, stats.total_rows
    assert_match(/CREATED_AT/, res.string)
  end

  def test_big_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new(2000)
    res = adapter.execute_stream 'select * from snowflake_sample_data.tpch_sf1.customer limit 5000', io, stats: stats
    # first line is headers
    assert_equal 5001, res.each_line.count
    assert_equal 5000, stats.total_rows
    assert_equal 2000, stats.data.size
    assert_match(/C_PHONE/, res.string)
  end

  def test_stream_with_block
    str = []
    adapter.stream('select * from posts') do
      str << it
    end
    assert_match(/First Post/, str.to_s)
  end

  def test_date_truncation
    date = adapter.date_literal '2025-08-06'
    %w[day week month quarter year].each do |unit|
      sql = "SELECT #{adapter.truncate_date(unit, date)}"
      res = adapter.execute(sql)
      matcher = res[0][0]
      case unit
      when 'day'
        assert_equal '2025-08-06', matcher.to_s
      when 'week'
        assert_equal '2025-08-04', matcher.to_s
      when 'month'
        assert_equal '2025-08-01', matcher.to_s
      when 'quarter'
        assert_equal '2025-07-01', matcher.to_s
      when 'year'
        assert_equal '2025-01-01', matcher.to_s
      end
    end

    adapter.alter_settings({ week_start_day: 'sunday' })
    res = adapter.execute("SELECT #{adapter.truncate_date('week', date)}")
    assert_equal '2025-08-03', res[0][0].to_s
  end

  def test_name_formats
    # full day name not possible with format strat
    # sql = "select #{adapter.extract_day_name(adapter.date_literal('2025-08-06'))}"
    # res = adapter.execute(sql)
    # assert_equal "#{adapter.adapter_name}: WEDNESDAY", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.extract_day_name(adapter.date_literal('2025-08-06'), abbreviate: true)}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: WED", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.extract_month_name(adapter.date_literal('2025-08-06'))}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: AUGUST", "#{adapter.adapter_name}: #{res[0][0].strip}"

    sql = "select #{adapter.extract_month_name(adapter.date_literal('2025-08-06'), abbreviate: true)}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: AUG", "#{adapter.adapter_name}: #{res[0][0].strip}"

    sql = "select #{adapter.extract_year_month(adapter.date_literal('2025-08-06'))}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: 202508", "#{adapter.adapter_name}: #{res[0][0]}"
  end

  def test_extracts
    r_date = Date.parse('2025-08-06')
    date = adapter.date_literal('2025-08-06')
    %w[year month].each do |unit|
      sql = "select #{adapter.send("extract_#{unit}".to_sym, date)}"
      res = adapter.execute sql
      assert_equal "#{adapter.adapter_name}: #{r_date.send(unit.to_sym)}", "#{adapter.adapter_name}: #{res[0][0]}"
    end
    sql = "select #{adapter.extract_year_month(date)}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: 202508", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.extract_day_of_month(date)}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: 6", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.extract_day_of_year(date)}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: 218", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.extract_week_of_year(date)}"
    res = adapter.execute(sql)
    assert_match(/#{adapter.adapter_name}: 3(1|2)/, "#{adapter.adapter_name}: #{res[0][0]}")
  end

  def test_nulls
    sql = "select #{adapter.if_null('null', "'yo'")}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: yo", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.null_if(0, 0)}, 1 as num"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: ", "#{adapter.adapter_name}: #{res[0][0]}"

    sql = "select #{adapter.cast("'2025-08-06'", 'date')}"
    res = adapter.execute(sql)
    assert_equal "#{adapter.adapter_name}: 2025-08-06", "#{adapter.adapter_name}: #{res[0][0]}"
  end

  def test_key_pair_auth
    adapter = DWH.create(:snowflake, {
                           auth_mode: 'kp',
                           account_identifier: 'MYACCOUNT',
                           username: 'test_user',
                           warehouse: 'COMPUTE_WH',
                           private_key: File.join(__dir__, '..', '..', 'snow_rsa_key.p8'),
                           database: 'TEST_DB'
                         })
    assert adapter.connect?
    assert_equal 2, adapter.tables(schema: 'public').size
  end

  # def test_oauth_strategy
  #   client_id = ''
  #   client_secret = ''
  #   tokens = {
  #     access_token: '',
  #     refresh_token: '',
  #     expires_at: Time.now - 200
  #   }
  #   a = DWH.create(:snowflake,
  #                  { auth_mode: 'oauth', account_identifier: 'MYACCOUNT-IDENTIFIER',
  #                    database: 'TEST_DB', oauth_client_id: client_id, oauth_client_secret: client_secret,
  #                    oauth_redirect_uri: 'https://localhost:3030' })
  #
  #   a.apply_oauth_tokens(**tokens)
  #
  #   res = a.execute('select 420')
  #   assert_equal '420', res[0][0]
  # end

  def test_get_tables_from_other_db
    tables = adapter.tables(schema: 'tpch_sf1', catalog: 'snowflake_sample_data')
    assert(tables.any? { it == 'CUSTOMER' })
  end
end
