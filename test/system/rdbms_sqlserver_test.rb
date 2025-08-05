require 'test_helper'

class RdbmsSqlServerTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:sqlserver,
                            { host: 'localhost', username: 'sa', password: 'TestPassword123!', database: 'test_db' })
  end

  def test_not_using_base
    refute adapter.class.using_base_settings?, 'should not be using base since we have mysql.yml'
    assert_equal '[hello]', adapter.quote('hello')
  end

  def test_basic_connection
    assert_equal 2, adapter.tables.size
    assert adapter.connect?
    failing = DWH.create(:sqlserver, { username: 'me', host: 'doesnotexist', database: 'doesntexist' })
    assert_raises DWH::ConnectionError do
      failing.connect!
    end
  end

  def test_get_table_stats
    stats = adapter.stats('posts', date_column: 'created_at')
    assert_equal 4, stats.row_count
    assert stats.date_start.is_a?(Date)
    assert stats.date_end.is_a?(Date)
    assert adapter.stats('users').row_count
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

  def test_get_tables_for_another_schema
    tbls = adapter.tables(catalog: 'msdb')
    assert_equal 223, tbls.size
    assert(tbls.any? { it[0] == 'sysdatatypemappings' })
  end

  def test_get_md_other_schema_table
    md = adapter.metadata('msdb.dbo.sysdatatypemappings')
    assert_equal 22, md.columns.size
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
