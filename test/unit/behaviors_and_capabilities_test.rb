require 'test_helper'

class BehaviorsAndCapabilitiesTest < Minitest::Test
  def setup
    @druid = DWH.create(:druid, host: 'localhost', port: 8080)
    @trino = DWH.create(:trino, host: 'localhost', catalog: 'dwh', username: 'me')
  end

  def test_join_behavior
    assert @druid.supports_table_join?
    assert_equal 'subquery', @druid.temp_table_type
    refute @druid.greedy_apply_date_filters

    assert_equal 'cte', @trino.temp_table_type
    assert @trino.supports_table_join?
  end

  def test_override_default_behavor
    d2 = DWH.create(:druid, host: 'localhost', port: 80, settings: { greedy_apply_date_filters: true })
    assert d2.greedy_apply_date_filters
  end

  def test_basic_capabilities
    refute @druid.supports_common_table_expressions?
    assert @trino.supports_common_table_expressions?
  end

  def test_array_behavior
    assert @druid.apply_advanced_filtering_on_array_projections?
    refute @trino.apply_advanced_filtering_on_array_projections?
  end

  def test_null_functions
    assert_equal 'NULLIF(sum(x), 0)', @druid.null_if('sum(x)', 0)
    assert_equal 'NVL(sum(x), sum(b))', @druid.if_null('sum(x)', 'sum(b)')
    assert_equal 'NULLIF(sum(x), 0)', @trino.null_if('sum(x)', 0)
    assert_equal 'COALESCE(sum(x), sum(b))', @trino.if_null('sum(x)', 'sum(b)')
  end

  def test_join_funcs
    assert_equal 'JOIN table_b ON 1=1', @druid.cross_join('table_b')
    assert_equal 'CROSS JOIN table_b', @trino.cross_join('table_b')
  end
end
