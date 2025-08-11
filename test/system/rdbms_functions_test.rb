require 'test_helper'

class RdbmsFunctions < Minitest::Test
  def adapters
    @adapters ||= [
      DWH.create(:trino, { host: 'localhost', username: 'ajo', catalog: 'tpch', schema: 'sf1' }),
      DWH.create(:mysql, { host: '127.0.0.1', username: 'test_user', password: 'test_password', database: 'test_db' }),
      DWH.create(:postgres, { host: 'localhost', username: 'test_user', password: 'test_password', database: 'test_db' }),
      DWH.create(:sqlserver, { host: 'localhost', username: 'sa', password: 'TestPassword123!', database: 'test_db' })
    ]
  end

  def test_date_truncation
    adapters.each do |adapter|
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
  end

  def test_name_formats
    adapters.each do |adapter|
      sql = "select #{adapter.extract_day_name(adapter.date_literal('2025-08-06'))}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: WEDNESDAY", "#{adapter.adapter_name}: #{res[0][0]}"
      sql = "select #{adapter.extract_month_name(adapter.date_literal('2025-08-06'))}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: AUGUST", "#{adapter.adapter_name}: #{res[0][0].strip}"
      sql = "select #{adapter.extract_year_month(adapter.date_literal('2025-08-06'))}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: 202508", "#{adapter.adapter_name}: #{res[0][0]}"
    end
  end

  def test_extracts
    adapters.each do |adapter|
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
  end

  def test_nulls
    adapters.each do |adapter|
      sql = "select #{adapter.if_null('null', "'yo'")}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: yo", "#{adapter.adapter_name}: #{res[0][0]}"

      sql = "select #{adapter.null_if(0, 0)}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: ", "#{adapter.adapter_name}: #{res[0][0]}"

      sql = "select #{adapter.cast("'2025-08-06'", 'date')}"
      res = adapter.execute(sql)
      assert_equal "#{adapter.adapter_name}: 2025-08-06", "#{adapter.adapter_name}: #{res[0][0]}"
    end
  end
end
