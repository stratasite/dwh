require 'test_helper'

class DruidTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:druid, { host: 'localhost', port: 8888 })
  end

  def test_connection_error
    error = assert_raises DWH::ConnectionError do
      druid = DWH.create(:druid, { open_timeout: 1, host: 'localhost_NOT' })
      druid.test_connection(raise_exception: true)
    end
    assert_match(/Failed to open/, error.message)

    druid = DWH.create(:druid, { open_timeout: 1, host: 'localhost_NOT' })
    refute druid.test_connection
  end

  def test_table_fetch
    assert_equal 2, adapter.tables.size
    refute adapter.table? 'simonsays'
    assert adapter.table? 'users'
  end

  def test_get_table_stats
    stats = adapter.stats('posts', date_column: '__time')
    assert_equal 4, stats.row_count
    assert stats.date_start.is_a?(Date)
    assert stats.date_end.is_a?(Date)
  end

  def test_can_get_metadata
    md = adapter.metadata('posts')
    assert_equal 7, md.columns.size
    col = md.find_column('__time')
    assert col, 'column should be found'
    assert_equal 'date_time', col.normalized_data_type
    boolcol = md.find_column('published')
    assert_equal 'string', boolcol.normalized_data_type
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
        assert_match(/Jane Smith,/, res) else
        assert_equal Faraday::Response, res.class
      end
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream 'select * from users', io, stats: stats
    assert_equal 4, res.each_line.count
    assert_equal 3, stats.total_rows
    assert_match(/email/, res.string, 'should include header')
  end

  def test_stream_with_block
    str = []
    adapter.stream('select * from posts') do
      str << it
    end
    assert_match(/First Post/, str.to_s)
  end
end
