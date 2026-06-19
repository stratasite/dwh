require 'test_helper'

# System tests for the ClickHouse adapter.
# Requires a running ClickHouse instance.
# Start with: docker compose -f test/support/compose.clickhouse.yml up -d
class ClickHouseSystemTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:clickhouse, {
      host: 'localhost',
      port: 8123,
      database: 'test_db'
    })
  end

  def test_ping
    assert adapter.connect?
  end

  def test_list_tables
    tables = adapter.tables
    assert_includes tables, 'users'
    assert_includes tables, 'posts'
  end

  def test_table_exists
    assert adapter.table?('users')
    refute adapter.table?('does_not_exist')
  end

  def test_metadata
    md = adapter.metadata('users')
    assert_equal 4, md.columns.size
    col = md.find_column('email')
    assert col, 'email column should be found'
    assert_equal 'string', col.normalized_data_type
  end

  def test_stats
    stats = adapter.stats('posts', date_column: 'created_at')
    assert_equal 4, stats.row_count
  end

  def test_execute_basic
    res = adapter.execute('SELECT 1')
    assert_equal '1', res[0][0].to_s
  end

  def test_execute_returns_correct_row_count
    res = adapter.execute('SELECT * FROM users')
    assert_equal 3, res.size
  end

  def test_execute_format_object
    res = adapter.execute('SELECT id, name FROM users ORDER BY id', format: :object)
    assert_equal Hash, res[0].class
    assert_equal '1', res[0]['id'].to_s
    assert_equal 'John Doe', res[0]['name']
  end

  def test_execute_format_csv
    res = adapter.execute('SELECT id, name FROM users ORDER BY id', format: :csv)
    assert_match(/id,name/, res)
    assert_match(/John Doe/, res)
  end

  def test_execute_bad_sql
    assert_raises DWH::ExecutionError do
      adapter.execute('SELECT * FROM this_table_does_not_exist')
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream('SELECT * FROM users', io, stats: stats)
    lines = res.each_line.to_a
    assert lines.size >= 4, 'should have header + 3 data rows'
    assert_match(/email/, res.string, 'should include column headers')
    assert_equal 3, stats.total_rows
  end

  def test_stream_with_block
    rows = []
    adapter.stream('SELECT id, name FROM users ORDER BY id') { |row| rows << row }
    assert_equal 3, rows.size
  end
end
