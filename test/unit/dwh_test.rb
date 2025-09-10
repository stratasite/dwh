# frozen_string_literal: true

require 'test_helper'

class TestDwh < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil DWH::VERSION
  end

  def test_table_stats_date_parsing
    s = DWH::TableStats.new(row_count: 1000, date_start: Time.now.to_s, date_end: (Time.now + 1).to_s)
    assert s.date_start.is_a?(DateTime)
  end

  def test_base_adapters_registered
    count = Dir.glob('lib/dwh/adapters/*.rb').count
    assert_equal count - 1, DWH.adapters.size

    assert DWH.adapter?(:snowflake)
  end

  def test_create_will_use_defaults
    # Testing issue where nil value is override the default
    adapter = DWH.create(:trino, { host: 'localhost', username: 'ajo', catalog: 'hive' })
    assert adapter

    begin
      DWH.create(:trino, { port: nil,
                           host: 'localhost', username: 'ajo', catalog: 'hive' })
    rescue StandardError => e
      assert_nil e, 'should not raise erro'
    end
  end
end
