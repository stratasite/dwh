require 'test_helper'

class CloudDatabricksTest < Minitest::Test
  TPCDS_TABLE = 'customer'.freeze
  TPCDS_KEY_COLUMN = 'c_customer_sk'.freeze
  TPCDS_STATS_TABLE = 'store_sales'.freeze
  STREAM_LIMIT = 10
  SCHEMA = 'tpcds_sf1'.freeze
  CATALOG = 'samples'.freeze
  HOST = 'workspace.cloud.databricks.com'.freeze
  WAREHOUSE = 'warehouse_id'.freeze
  OAUTH_CLIENT_ID = ''.freeze
  OAUTH_CLIENT_SECRET = ''.freeze

  def adapter
    @adapter ||=
      DWH.create(:databricks,
                 {
                   host: HOST,
                   warehouse: WAREHOUSE,
                   oauth_client_id: OAUTH_CLIENT_ID,
                   oauth_client_secret: OAUTH_CLIENT_SECRET,
                   catalog: CATALOG,
                   schema: SCHEMA
                 })
  end

  def test_basic_connection
    assert adapter.connect?
  end

  def test_get_table_list
    res = adapter.tables(schema: SCHEMA)
    assert res.is_a?(Array)
    assert res.any? { |name| name.to_s.downcase == TPCDS_TABLE.downcase },
           "Expected '#{TPCDS_TABLE}' in tables list"
  end

  def test_get_table_stats
    stats = adapter.stats(TPCDS_STATS_TABLE)
    assert stats.row_count >= 0
  end

  def test_can_get_metadata
    md = adapter.metadata(TPCDS_TABLE)
    assert md.columns.size.positive?
    col = md.find_column(TPCDS_KEY_COLUMN)
    assert col, 'column should be found'
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
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream("SELECT * FROM #{TPCDS_TABLE} LIMIT #{STREAM_LIMIT}", io, stats: stats)
    assert res.each_line.count.positive?
    assert_match(/\w+/, res.string)
  end

  def test_stream_with_block
    rows = []
    adapter.stream("SELECT * FROM #{TPCDS_TABLE} LIMIT #{[STREAM_LIMIT, 5].min}") do
      rows << it
    end
    assert rows.size.positive?
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
end
