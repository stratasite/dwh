# frozen_string_literal: true

require 'test_helper'

class FactoryPoolTest < Minitest::Test
  # Minimal adapter for pool tests — no real database.
  # Tracks instances and close calls so we can assert shutdown behavior.
  class FakeAdapter < DWH::Adapters::Adapter
    class << self
      attr_accessor :created_count, :closed_count, :instances

      def reset!
        @created_count = 0
        @closed_count = 0
        @instances = []
      end
    end

    attr_reader :closed

    def initialize(config = {})
      super
      @closed = false
      self.class.created_count += 1
      self.class.instances << self
    end

    def execute(_sql)
      [1]
    end

    def close
      @closed = true
      self.class.closed_count += 1
      @connection = nil
    end
  end

  def setup
    FakeAdapter.reset!
    DWH.register('fake_adapter', FakeAdapter)
    # Isolate pool map for each test
    DWH.instance_variable_set(:@pools, {})
  end

  def teardown
    DWH.shutdown
    DWH.unregister('fake_adapter')
    DWH.instance_variable_set(:@pools, {})
  end

  def make_pool(name = 'test-pool', size: 2, timeout: 1)
    DWH.pool(name, 'fake_adapter', {}, timeout: timeout, size: size)
  end

  # --- shutdown ---

  def test_shutdown_by_string_name_removes_pool_and_closes_live_connection
    pool = make_pool('by-string')
    # Check out and return so a real connection exists (the c.close NameError case)
    conn = pool.with { |c| c }
    refute conn.closed

    DWH.shutdown('by-string')

    refute DWH.pools.key?('by-string')
    assert conn.closed, 'checked-out-and-returned connection should be closed'
    assert_equal 1, FakeAdapter.closed_count
  end

  def test_shutdown_by_symbol_name_removes_pool_and_closes_live_connection
    pool = make_pool('by-symbol')
    conn = pool.with { |c| c }

    DWH.shutdown(:'by-symbol')

    refute DWH.pools.key?('by-symbol')
    assert conn.closed
  end

  def test_pool_stores_symbol_name_as_string_and_shutdown_finds_it
    # pool used to store the key as given; shutdown looked up to_s — Symbol entries survived.
    pool = DWH.pool(:sym_key, 'fake_adapter', {}, timeout: 1, size: 2)
    conn = pool.with { |c| c }

    assert DWH.pools.key?('sym_key'), 'pool name must be normalized to String'
    refute DWH.pools.key?(:sym_key)

    DWH.shutdown(:sym_key)

    refute DWH.pools.key?('sym_key')
    assert conn.closed
  end

  def test_pool_symbol_and_string_names_share_the_same_pool
    a = DWH.pool(:shared, 'fake_adapter', {}, timeout: 1, size: 2)
    b = DWH.pool('shared', 'fake_adapter', {}, timeout: 1, size: 2)

    assert a.equal?(b)
    assert_equal 1, DWH.pools.size
  end

  def test_shutdown_by_pool_object_removes_pool_and_closes_live_connection
    pool = make_pool('by-object')
    conn = pool.with { |c| c }

    DWH.shutdown(pool)

    refute DWH.pools.key?('by-object')
    assert conn.closed
  end

  def test_shutdown_all_pools_clears_map_and_closes_connections
    p1 = make_pool('all-a')
    p2 = make_pool('all-b')
    c1 = p1.with { |c| c }
    c2 = p2.with { |c| c }

    DWH.shutdown

    assert_empty DWH.pools
    assert c1.closed
    assert c2.closed
  end

  def test_shutdown_unknown_name_does_not_raise
    DWH.shutdown('does-not-exist')
    DWH.shutdown(:also_missing)
  end

  def test_shutdown_unused_pool_still_removes_entry
    make_pool('unused')
    assert DWH.pools.key?('unused')

    DWH.shutdown('unused')

    refute DWH.pools.key?('unused')
    # Never created a connection — close not required, but must not raise
    assert_equal 0, FakeAdapter.closed_count
  end

  # --- concurrent creation ---

  def test_concurrent_pool_calls_return_same_object
    results = []
    mutex = Mutex.new
    threads = 20.times.map do
      Thread.new do
        p = DWH.pool('race-pool', 'fake_adapter', {}, timeout: 1, size: 5)
        mutex.synchronize { results << p }
      end
    end
    threads.each(&:join)

    assert_equal 20, results.size
    assert(results.all? { |p| p.equal?(results.first) }, 'all threads should get the same pool object')
    assert_equal 1, DWH.pools.size
  end

  # --- reaper ---

  def test_reaper_tick_does_not_create_connections
    pool = make_pool('reaper-stats', size: 3)
    assert_equal 0, FakeAdapter.created_count
    assert_equal 0, pool.idle

    DWH.reaper_tick(300)

    assert_equal 0, FakeAdapter.created_count,
                 'reaper stats logging must not check out / create connections'
    assert_equal 0, pool.idle
  end

  def test_reaper_tick_survives_shut_down_pool
    pool = make_pool('reaper-dead')
    pool.with { |c| c }
    # Shut the pool object but leave a stale map entry to simulate race
    pool.shutdown { it.close }
    DWH.pools['reaper-dead'] = pool

    # Must not raise — reaper rescues PoolShuttingDownError
    DWH.reaper_tick(300)
  end
end
