require 'connection_pool'

module DWH
  # Manages adapters. This should be the means by which adapters
  # are created, loaded, and pooled.
  module Factory
    include Logger

    # Register your new adapter.
    # @param adapter_name [String, Symbol] your adapter name. Could be different from
    #   the class name.
    # @param adapter_class [Class] actual class of the adapter.
    def register(adapter_name, adapter_class)
      raise ConfigError, 'adapter_class should be a class' unless adapter_class.is_a?(Class)

      adapter_class.load_settings
      adapters[adapter_name.to_sym] = adapter_class
    end

    # Remove the given adapter from the registry.
    def unregister(adapter_name)
      adapters.delete adapter_name.to_sym
    end

    # Get the adapter.
    # @param adapter_name [String, Symbol]
    def get_adapter(adapter_name)
      raise "Adapter '#{adapter_name}' not found. Did you forget to register it: DWH.register(MyAdapterClass)" unless adapter?(adapter_name)

      adapters[adapter_name.to_sym]
    end

    # Check if the given adapter is registered
    # @param adapter_name [String, Symbol]
    def adapter?(adapter_name)
      adapters.key?(adapter_name.to_sym)
    end

    # Get the list of registed adapters
    def adapters
      @adapters ||= {}
    end

    # Current active pools
    def pools
      @pools ||= {}
    end

    # Mutex guarding pool creation / map mutation.
    def pool_mutex
      @pool_mutex ||= Mutex.new
    end

    # The canonical way of creating an adapter instance
    # in DWH.
    # @param adapter_name [String, Symbol]
    # @param config [Hash] options hash for the target database
    #
    # @example connect to MySQL
    #   DWH.create(:mysql, { host: '127.0.0.1', databse: 'mydb', username: 'me', password: 'mypwd', client_name: 'Strata CLI'})
    # @example connect Trino
    #   DWH.create(:trino, {host: 'localhost', catalog: 'native', username: 'Ajo'})
    # @example connect to Druid
    #   DWH.create(:druid, {host: 'localhost',port: 8080, protocol: 'http'})
    def create(adapter_name, config)
      get_adapter(adapter_name).new(config)
    end

    # Create a pool of connections for a given name and adapter.
    # Returns existing pool if it was already created.
    #
    # @param name [String, Symbol] custom name for your pool (stored as String)
    # @param adapter_name [String, Symbol]
    # @param config [Hash] connection options
    # @param timeout [Integer] pool checkout time out
    # @param size [Integer] size of the pool
    def pool(name, adapter_name, config, timeout: 5, size: 10)
      name = name.to_s
      pool_mutex.synchronize do
        if pools.key?(name)
          pools[name]
        else
          pools[name] = ConnectionPool.new(size: size, timeout: timeout) do
            create(adapter_name, config)
          end
        end
      end
    end

    # Shutdown a specific pool or all pools
    # @param pool [String, Symbol, ConnectionPool, nil] pool or name of pool
    #   or nil to shut everything down
    def shutdown(pool = nil)
      # Mutate the map under the same mutex as pool creation so a concurrent
      # create cannot be orphaned by @pools = {} / delete racing it.
      # Close outside the lock — ConnectionPool#shutdown can wait on check-in.
      to_close = pool_mutex.synchronize do
        case pool
        when String, Symbol
          # Delete first so a raising close cannot leave a dead pool in the map.
          removed = pools.delete(pool.to_s)
          removed ? [removed] : []
        when ConnectionPool
          key = pools.key(pool)
          pools.delete(key) if key
          [pool]
        else
          closing = pools.values
          @pools = {}
          closing
        end
      end
      to_close.each { |p| p.shutdown { it.close } }
    end

    # Start reaper that will periodically clean up
    # unused or idle connections.
    # @param frequency [Integer] defaults to 300 seconds
    # @return [Thread] the reaper thread
    def start_reaper(frequency = 300)
      logger.info 'Starting DB Adapter reaper process'
      Thread.new do
        loop do
          reaper_tick(frequency)
          sleep frequency
        end
      end
    end

    # One reaper cycle: log pool stats (without checking out) and reap idle connections.
    # Safe to call from tests; rescues per-pool so a shut-down pool cannot kill the loop.
    # @param frequency [Integer] idle threshold passed to ConnectionPool#reap
    def reaper_tick(frequency = 300)
      # Snapshot so concurrent shutdown deletions do not mutate while we iterate.
      pools.to_a.each do |name, pool|
        logger.info "DB POOL FOR #{name} STATS:"
        logger.info "\tSize:      #{pool.size}"
        logger.info "\tIdle:      #{pool.idle}"
        logger.info "\tAvailable: #{pool.available}"
        pool.reap(frequency) { it.close }
      rescue ConnectionPool::PoolShuttingDownError => e
        logger.info "Skipping reaper for pool #{name}: #{e.class}"
      rescue StandardError => e
        logger.error "Reaper error for pool #{name}: #{e.class}: #{e.message}"
      end
    end
  end
end
