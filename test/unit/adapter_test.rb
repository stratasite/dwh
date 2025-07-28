require "test_helper"

class AdapterTest < Minitest::Test
  class MyDB < DWH::Adapters::Adapter
    define_config :db_name, required: true
    define_config :schema, default: "public"
    define_config :port, required: true, default: 3001

    def execute_stream(_sql, _io, stats:)
      5.times do |i|
        stats << [i, 0, 0]
      end
    end
  end

  def setup
    MyDB.load_settings
    @adapter = MyDB.new db_name: "base_adapter_db"
  end

  def test_define_config
    mydb = MyDB.new(db_name: "not_default_db")
    assert_equal "public", MyDB.config_definitions[:schema][:default]
    assert_equal "public", mydb.config[:schema], "instance should use default value"
    assert_equal "not_default_db", mydb.config[:db_name]

    assert_raises DWH::ConfigError do
      MyDB.new({schema: "ewh"})
    end
  end

  def test_adapter_is_using_base
    MyDB.load_settings
    assert MyDB.using_base_settings?, "should be using base since it doesnt have a custom file"
  end

  def test_correct_date_output
    f = @adapter.date_format_sql("'#{Date.today}'", "yyyyMMdd")
    assert_equal "DATE_FORMAT('#{Date.today}', 'yyyyMMdd')", f
  end

  def test_ruby_date_to_literal_conversion
    o = @adapter.date_lit(Date.today)
    assert_equal "'#{Date.today.strftime("%Y-%m-%d")}'", o

    n = Time.now
    o = @adapter.timestamp_lit(n)
    assert_equal "TIMESTAMP '#{n.strftime("%Y-%m-%d %H:%M:%S")}'", o
  end

  def test_upper_lower_trim
    assert_equal @adapter.lower_case("some_col"), "LOWER(some_col)"
    assert_equal @adapter.upper_case("some_col"), "UPPER(some_col)"
    assert_equal @adapter.trim("some_col"), "TRIM(some_col)"
  end

  def test_lits_and_quotes
    assert_equal '"my_col"', @adapter.quote("my_col")
    assert_equal "'myString'", @adapter.string_lit("myString")
  end

  def test_streaming_stats
    mdb = MyDB.new db_name: "dwh"
    stats = DWH::StreamingStats.new(4)
    mdb.execute_stream("nil", StringIO.new, stats: stats)

    assert_equal 4, stats.data.size
    assert_equal 5, stats.total_rows
    assert_equal 9, stats.max_row_size
  end
end
