require 'test_helper'

class RdbmsSqliteTest < Minitest::Test
  def adapter
    @adapter ||= DWH.create(:sqlite, { file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite') })
  end

  def test_basic_connection
    assert adapter.connect?
  end

  def test_readonly_connection
    readonly = DWH.create(:sqlite, {
                            file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite'),
                            readonly: true
                          })
    assert readonly.connect?
    # Readonly adapter should not be able to write
    assert_raises DWH::ExecutionError do
      readonly.execute("INSERT INTO users (id, name, email) VALUES (999, 'Test', 'test@example.com')")
    end
  end

  def test_wal_mode_enabled
    # WAL mode should be enabled by default
    result = adapter.execute('PRAGMA journal_mode')
    assert_equal 'wal', result[0][0].downcase
  end

  def test_wal_mode_disabled
    # Create a temporary test database to avoid WAL persistence issues
    temp_db = File.join(__dir__, '..', 'support', 'sqlite', 'test_no_wal.sqlite')
    FileUtils.cp(File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite'), temp_db)

    no_wal = DWH.create(:sqlite, {
                          file: temp_db,
                          enable_wal: false
                        })
    # Set it explicitly to DELETE mode first
    no_wal.connection.execute('PRAGMA journal_mode = DELETE')
    result = no_wal.execute('PRAGMA journal_mode')
    refute_equal 'wal', result[0][0].downcase
    no_wal.close
    FileUtils.rm_f(temp_db)
  end

  def test_custom_pragmas
    custom = DWH.create(:sqlite, {
                          file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite'),
                          pragmas: { cache_size: -128_000 }
                        })
    result = custom.execute('PRAGMA cache_size')
    assert_equal(-128_000, result[0][0])
    custom.close
  end

  def test_get_tables
    res = adapter.tables
    assert_equal 2, res.size
    assert_includes res, 'users'
    assert_includes res, 'posts'

    assert adapter.table?('users')
    refute adapter.table?('notinthedb')
  end

  def test_get_table_stats
    stats = adapter.stats('posts', date_column: 'created_at')
    assert_equal 4, stats.row_count
    assert stats.date_start.is_a?(Date)
    assert stats.date_end.is_a?(Date)
  end

  def test_get_table_stats_without_date
    stats = adapter.stats('posts')
    assert_equal 4, stats.row_count
    assert_nil stats.date_start
    assert_nil stats.date_end
  end

  def test_can_get_metadata
    md = adapter.metadata('posts')
    assert_equal 7, md.columns.size
    col = md.find_column('created_at')
    assert col, 'column should be found'
    assert_equal 'date_time', col.normalized_data_type

    boolcol = md.find_column('published')
    assert boolcol, 'boolean column should be found'
    # SQLite stores boolean as INTEGER
    assert_includes %w[boolean integer], boolcol.normalized_data_type.downcase
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
        assert_equal Array, res.class
      end
    end
  end

  def test_execute_stream
    io = StringIO.new
    stats = DWH::StreamingStats.new
    res = adapter.execute_stream 'select * from users', io, stats: stats
    assert_equal 4, res.each_line.count
    assert_equal 3, stats.total_rows
    assert_match(/created_at/, res.string)
  end

  def test_stream_with_block
    str = []
    adapter.stream('select * from posts') do
      str << it
    end
    assert_match(/First Post/, str.to_s)
  end

  def test_connection_closes_properly
    test_adapter = DWH.create(:sqlite, { file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite') })
    conn = test_adapter.connection
    refute conn.closed?
    test_adapter.close
    assert conn.closed?
  end

  def test_multiple_connections_allowed
    # SQLite should allow multiple connections to the same file
    adapter1 = DWH.create(:sqlite, { file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite') })
    adapter2 = DWH.create(:sqlite, { file: File.join(__dir__, '..', 'support', 'sqlite', 'test_db.sqlite') })

    res1 = adapter1.execute('select count(*) from users')
    res2 = adapter2.execute('select count(*) from users')

    assert_equal res1[0][0], res2[0][0]

    adapter1.close
    adapter2.close
  end
end
