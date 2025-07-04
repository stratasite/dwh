require "test_helper"


class CustomTest < Minitest::Test

  class CustomAdapter < DWH::Adapters::Adapter
    def execute(sql)
      puts "hello"
      [1]
    end
  end

  def setup
    DWH.register("custom_adapter", CustomAdapter)
  end

  def test_custom_adapter_can_be_registered
    assert DWH.has_adapter?"custom_adapter"
  end

  def test_custom_adapter_exec
    pool = DWH.pool('custom', 'custom_adapter', {})
    res = pool.with do |conn|
      refute File.exist?(conn.class.settings_file), "file should not exist"
      conn.execute("SELECT 1")
    end

    assert res[0] == 1
  end

  class LoadedAdapter < DWH::Adapters::Adapter
    def self.settings_file
      @settings_file ||= File.join(__dir__, "custom_settings.yml")
    end
  end

  def test_enable_custom_settings_file
    LoadedAdapter.settings_file = File.join __dir__, "custom_settings.yml"
    DWH.register("custom_settings", LoadedAdapter)
    pool = DWH.pool('custom-settings', 'custom_settings', {})
    conn = pool.checkout
    assert_equal "blah-blah", conn.date_format
  end

end
