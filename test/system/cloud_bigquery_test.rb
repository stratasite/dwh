require 'test_helper'

# Live BigQuery tests. Uses Application Default Credentials, either from
# `gcloud auth application-default login` or a service-account keyfile via
# GOOGLE_APPLICATION_CREDENTIALS, and expects a fixture dataset:
#
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json BIGQUERY_PROJECT=my-project \
#     BUNDLE_WITH=development bundle exec ruby -Itest test/system/cloud_bigquery_test.rb
#
#   CREATE TABLE `strata_test.users` (id INT64, name STRING, email STRING, created_at DATE);
#   INSERT INTO `strata_test.users` VALUES
#     (1, 'John Doe', 'john@example.com', DATE '2024-01-15'),
#     (2, 'Jane Smith', 'jane@example.com', DATE '2024-01-16'),
#     (3, 'Bob Johnson', 'bob@example.com', DATE '2024-01-17');
#   CREATE TABLE `strata_test.posts` (id INT64, user_id INT64, title STRING, body STRING,
#                                      published BOOL, views INT64, created_at DATE);
#   INSERT INTO `strata_test.posts` VALUES
#     (1, 1, 'First Post', 'Hello', true, 10, DATE '2024-01-15'),
#     (2, 1, 'Second Post', 'World', false, 0, DATE '2024-01-16'),
#     (3, 2, 'Jane Post', 'Hi', true, 5, DATE '2024-01-17'),
#     (4, 3, 'Bob Post', 'Yo', true, 2, DATE '2024-01-18');
class CloudBigQueryTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:bigquery, {
                              project_id: ENV.fetch('BIGQUERY_PROJECT'),
                              dataset: ENV.fetch('BIGQUERY_DATASET', 'strata_test')
                            })
  end

  def test_connection
    assert adapter.connect?
  end

  def test_tables
    assert_equal %w[posts users], adapter.tables.sort
    assert adapter.table?('users')
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
    assert_equal 'date', md.find_column('created_at').normalized_data_type
    assert_equal 'boolean', md.find_column('published').normalized_data_type
    assert_equal 'bigint', md.find_column('views').normalized_data_type
  end

  def test_execute_basic
    assert_equal 1, adapter.execute('select 1')[0][0]
  end

  def test_execute_bad_sql
    assert_raises(DWH::ExecutionError) { adapter.execute('select safsdasdf') }
    assert_raises(DWH::ExecutionError) { adapter.execute('select * from table_does_not_exist') }
  end

  def test_execute_formats
    %i[array object csv native].each do |format|
      res = adapter.execute('select * from users', format: format)
      case format
      when :array then assert_equal Array, res[0].class
      when :object then assert_equal Hash, res[0].class
      when :csv then assert_match(/Jane\sSmith/, res)
      else assert_equal Hash, res.class
      end
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream 'select * from users', io, stats: stats
    assert_equal 4, res.each_line.count # header + 3 rows
    assert_equal 3, stats.total_rows
    assert_match(/created_at/, res.string)
  end

  def test_stream_with_block
    rows = []
    adapter.stream('select * from posts') { rows << it }
    assert_equal 4, rows.size
    assert_match(/First Post/, rows.to_s)
  end

  def test_date_truncation
    date = adapter.date_literal '2025-08-06'
    { 'day' => '2025-08-06', 'week' => '2025-08-04', 'month' => '2025-08-01',
      'quarter' => '2025-07-01', 'year' => '2025-01-01' }.each do |unit, expected|
      assert_equal expected, adapter.execute("SELECT #{adapter.truncate_date(unit, date)}")[0][0].to_s, unit
    end

    adapter.alter_settings({ week_start_day: 'sunday' })
    assert_equal '2025-08-03', adapter.execute("SELECT #{adapter.truncate_date('week', date)}")[0][0].to_s
  ensure
    adapter.reset_settings
  end

  def test_name_formats
    date = adapter.date_literal('2025-08-06')
    assert_equal 'WEDNESDAY', adapter.execute("select #{adapter.extract_day_name(date)}")[0][0]
    assert_equal 'WED', adapter.execute("select #{adapter.extract_day_name(date, abbreviate: true)}")[0][0]
    assert_equal 'AUGUST', adapter.execute("select #{adapter.extract_month_name(date)}")[0][0]
    assert_equal 'AUG', adapter.execute("select #{adapter.extract_month_name(date, abbreviate: true)}")[0][0]
    assert_equal 202_508, adapter.execute("select #{adapter.extract_year_month(date)}")[0][0]
  end

  def test_extracts
    date = adapter.date_literal('2025-08-06')
    assert_equal 2025, adapter.execute("select #{adapter.extract_year(date)}")[0][0]
    assert_equal 8, adapter.execute("select #{adapter.extract_month(date)}")[0][0]
    assert_equal 6, adapter.execute("select #{adapter.extract_day_of_month(date)}")[0][0]
    assert_equal 218, adapter.execute("select #{adapter.extract_day_of_year(date)}")[0][0]
    assert_includes [31, 32], adapter.execute("select #{adapter.extract_week_of_year(date)}")[0][0]
  end

  def test_nulls
    assert_equal 'yo', adapter.execute("select #{adapter.if_null('null', "'yo'")}")[0][0]
    assert_nil adapter.execute("select #{adapter.null_if(0, 0)}, 1 as num")[0][0]
    assert_equal '2025-08-06', adapter.execute("select #{adapter.cast("'2025-08-06'", 'date')}")[0][0].to_s
  end
end
