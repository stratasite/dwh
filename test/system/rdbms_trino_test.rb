require 'test_helper'

class RdbmsTrinoTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:trino, { host: 'localhost', username: 'ajo', catalog: 'tpch', schema: 'sf1' })
  end

  def test_basic_connection
    assert adapter.connect?
    failing = DWH.create(:trino, { host: 'doesnotexist', catalog: 'notnative', username: 'not_ajo' })
    assert_raises DWH::ConnectionError do
      failing.connect!
    end
  end

  def test_get_tables
    res = adapter.tables schema: 'sf1'
    assert_equal 8, res.size

    assert adapter.table?('customer')
    refute adapter.table?('notinthedb')
  end

  def test_get_table_stats
    stats = adapter.stats('lineitem', date_column: 'commitdate')
    assert_equal 6_001_215, stats.row_count
    assert stats.date_start.is_a?(Date)
    assert stats.date_end.is_a?(Date)
    assert adapter.stats('customer').row_count
  end

  def test_can_get_metadata
    md = adapter.metadata('lineitem')
    assert_equal 16, md.columns.size

    col = md.find_column('shipmode')
    assert col, 'column should be found'
    assert_equal 'string', col.normalized_data_type
    boolcol = md.find_column('partkey')
    assert_equal 'bigint', boolcol.normalized_data_type
  end

  def test_get_tables_for_another_schema
    tbls = adapter.tables(schema: 'tiny')
    assert_equal 8, tbls.size
    assert(tbls.any? { it == 'nation' })
  end

  def test_get_md_other_schema_table
    md = adapter.metadata('tiny.nation')
    assert_equal 4, md.columns.size
  end

  def test_execute_basic
    res = adapter.execute('select 7')
    assert_equal 7, res[0][0].to_i
  end

  def test_execute_bad_sql
    assert_raises DWH::ExecutionError do
      adapter.execute('select safsdasdf', retries: 0)
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
        assert_match(/John Doe,/, res)
      else
        assert_equal TinyTds::Result, res.class
      end
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream 'select * from users', io, stats: stats
    assert_equal 3, res.each_line.count
    assert_equal 3, stats.total_rows
  end

  def test_stream_with_block
    str = []
    adapter.stream('select * from posts') do
      str << it
    end
    assert_match(/First Post/, str.to_s)
  end

  def test_date_truncation
    date = adapter.date_lit '2025-08-06'
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
