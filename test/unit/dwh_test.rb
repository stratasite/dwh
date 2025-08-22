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
end
