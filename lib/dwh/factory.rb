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
    # @param name [String] custom name for your pool
    # @param adapter_name [String, Symbol]
    # @param config [Hash] connection options
    # @param timeout [Integer] pool checkout time out
    # @param size [Integer] size of the pool
    def pool(name, adapter_name, config, timeout: 5, size: 10)
      if pools.key?(name)
        pools[name]
      else
        pools[name] = ConnectionPool.new(size: size, timeout: timeout) do
          create(adapter_name, config)
        end
      end
    end

    # Shutdown a specific pool or all pools
    # @param pool [String, ConnectionPool, nil] pool or name of pool
    #   or nil to shut everything down
    def shutdown(pool = nil)
      case pool.class
      when String
        pools[pool].shutdown { it.close }
        pools.delete(pool)
      when Symbol
        pools[pool.to_s].shutdown { it.close }
        pools[pool.to_s].delete
      when ConnectionPool
        pool.shutdown { it.close }
        pools.delete(pools.key(pool))
      else
        pools.each_value do |val|
          val.shutdown { c.close }
        end
        @pools = {}
      end
    end

    # Start reaper that will periodically clean up
    # unused or idle connections.
    # @param frequency [Integer] defaults to 300 seconds
    def start_reaper(frequency = 300)
      logger.info 'Starting DB Adapter reaper process'
      Thread.new do
        loop do
          pools.each do |name, pool|
            logger.info "DB POOL FOR #{name} STATS:"
            pool.with do
              logger.info "\tSize:      #{pool.size}"
              logger.info "\tIdle:      #{pool.available}"
              logger.info "\tAvailable: #{pool.available}"
            end
            pool.reap(frequency) { it.close }
          end
          sleep frequency
        end
      end
    end
  end
end
