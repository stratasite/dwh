require 'test_helper'

class PostgresTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:postgres, {
                              host: 'localhost',
                              username: 'test_user',
                              password: 'test_password',
                              database: 'test_db'
                            })
  end

  def test_basic_connection
    assert_equal 2, adapter.tables.size
  end

  def test_basic_with_nil_schema
    adapter = DWH.create(:postgres, {
                           host: 'localhost',
                           username: 'test_user',
                           schema: nil,
                           password: 'test_password',
                           database: 'test_db'
                         })
    assert_equal 210, adapter.tables.size
    refute adapter.table? 'simonsays'
    assert adapter.table? 'users'
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

  def test_get_md_other_schema_table
    md = adapter.metadata('pg_stats', schema: 'pg_catalog')
    assert_equal 14, md.columns.size
  end

  def test_execute_basic
  end

  def test_execute_formats
  end

  def test_execute_stream
  end

  def test_stream_with_block
  end

  def test_ssl_connection
  end

  def test_send_user_context_and_app_info
  end
end
