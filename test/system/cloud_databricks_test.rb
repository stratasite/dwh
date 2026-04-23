require 'test_helper'

class CloudDatabricksTest < Minitest::Test
  def adapter
    @adapter ||=
      DWH.create(:databricks,
                 {
                   host: 'MYWORKSPACE.cloud.databricks.com',
                   warehouse: 'test_warehouse_id',
                   oauth_client_id: '',
                   oauth_client_secret: '',
                   catalog: 'main',
                   schema: 'default'
                 })
  end

  def test_basic_connection
    assert adapter.connect?
  end

  def test_get_table_list
    res = adapter.tables(schema: 'default')
    assert res.is_a?(Array)
  end

  def test_get_table_stats
    skip 'Requires a real test table with data'
    stats = adapter.stats('test_table', date_column: 'created_at')
    assert stats.row_count >= 0
    assert stats.date_start.is_a?(Date) if stats.date_start
    assert stats.date_end.is_a?(Date) if stats.date_end
  end

  def test_can_get_metadata
    skip 'Requires a real test table'
    md = adapter.metadata('test_table')
    assert md.columns.size > 0
    col = md.find_column('id')
    assert col, 'column should be found'
  end

  def test_get_tables_from_other_catalog
    skip 'Requires access to multiple catalogs'
    tables = adapter.tables(catalog: 'samples', schema: 'tpch')
    assert tables.is_a?(Array)
  end

  def test_execute_basic
    res = adapter.execute('SELECT 1')
    assert_equal 1, res[0][0].to_i
  end

  def test_execute_bad_sql
    assert_raises DWH::ExecutionError do
      adapter.execute('SELECT invalid_syntax_here')
    end

    assert_raises DWH::ExecutionError do
      adapter.execute('SELECT * FROM table_does_not_exist_12345')
    end
  end

  def test_execute_formats
    %i[array object csv native].each do |format|
      res = adapter.execute('SELECT 1 AS num, "test" AS str', format: format)
      case format
      when :array
        assert_equal Array, res[0].class
      when :object
        assert_equal Hash, res[0].class
      when :csv
        assert_match(/num/, res)
      else
        assert_equal Hash, res.class
      end
    end
  end

  def test_execute_stream
    skip 'Requires a real test table with data'
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream('SELECT * FROM test_table LIMIT 10', io, stats: stats)
    assert res.each_line.count > 0
    assert_match(/\w+/, res.string)
  end

  def test_stream_with_block
    skip 'Requires a real test table with data'
    rows = []
    adapter.stream('SELECT * FROM test_table LIMIT 5') do
      rows << it
    end
    assert rows.size > 0
  end

  def test_requires_oauth_client_credentials
    assert_raises DWH::ConfigError do
      DWH.create(:databricks, {
                   host: 'test.cloud.databricks.com',
                   warehouse: 'warehouse123'
                   # missing oauth_client_id and oauth_client_secret
                 })
    end
  end

  def test_oauth_m2m_flow
    # This test validates OAuth M2M auth works with valid credentials
    # Skip for CI unless credentials are configured
    skip 'Requires valid Databricks OAuth credentials'

    adapter = DWH.create(:databricks, {
                           host: 'MYWORKSPACE.cloud.databricks.com',
                           warehouse: 'test_warehouse_id',
                           oauth_client_id: 'test_client_id',
                           oauth_client_secret: 'test_client_secret',
                           catalog: 'main'
                         })
    assert adapter.connect?
    assert adapter.tables.is_a?(Array)
  end
end
