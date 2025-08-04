require 'test_helper'

class RdbmsMySqlTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:mysql, {
                              host: '127.0.0.1', username: 'test_user',
                              password: 'test_password', database: 'test_db'
                            })
  end

  def test_basic_connection
    assert_equal 2, adapter.tables.size
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
    assert_equal 'integer', boolcol.normalized_data_type
  end

  def test_get_tables_for_another_schema
    tbls = adapter.tables(schema: 'performance_schema')
    assert_equal 8, tbls.size
    assert(tbls.any? { it[0] == 'global_status' })
  end

  def test_get_md_other_schema_table
    md = adapter.metadata('information_schema.tables')
    assert_equal 21, md.columns.size
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
        assert_equal Mysql2::Result, res.class
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

  def test_ssl_connection
    ssl = DWH.create(:mysql,
                     {
                       host: '127.0.0.1', username: 'test_user',
                       password: 'test_password', database: 'test_db', ssl: true
                     })
    assert_equal 2, ssl.tables.size
  end
end
