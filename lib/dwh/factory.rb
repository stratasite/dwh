require 'connection_pool'

module DWH
  module Factory
    include Logger

    def register(adapter_name, adapter_class)
      adapter_class.load_settings
      adapters[adapter_name.to_sym] = adapter_class
    end

    def get_adapter(adapter_name)
      if has_adapter?(adapter_name)
        adapters[adapter_name.to_sym]
      else
        raise "Adapter '#{adapter_name}' not found. Did you forget to register it: DWH.register(MyAdapterClass)"
      end
    end

    def has_adapter?(adapter_name)
      adapters.has_key?(adapter_name.to_sym)
    end

    def adapters
      @adapters ||= {}
    end

    def pools
      @pools ||= {}
    end

    def create(adapter_name, config)
      get_adapter(adapter_name).new(config)
    end

    def pool(name, adapter_name, config, timeout: 5, size: 10)
      if pools.key?(name)
        pools[name]
      else
        pools[name] = ConnectionPool.new(size: size, timeout: timeout) do
          get_adapter(adapter_name).new(config)
        end
      end
    end

    def start_reaper(frequency = 300)
      logger.info "Starting DB Adapter reaper process"
      # FIXME: the gem added reap methods but didnt release it yet.
      Thread.new do
        loop do
          pools.each do |name, pool|
            logger.info "DB POOL FOR #{name} STATS:"
            pool.with do
              logger.info "\tSize:      #{pool.size}"
              logger.info "\tAvailable: #{pool.available}"
            end
            # pool.reap(frequency) { |conn| conn.close }
          end
          sleep frequency
        end
      end
    end
  end
end
