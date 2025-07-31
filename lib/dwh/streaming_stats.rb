module DWH
  # Basic streaming stats collector.  This is used when runing a query
  # via execute_streaming method.  As the data comes in it will write
  # to the stats object. This can be read in another thread to
  # update a UI.
  class StreamingStats
    # Most cases the streaming data will be streamed to a tempfile
    # rather than memory. In those cases, we will keep a limited amount
    # of data in memory.  This is for previewing or other quick intropsections.
    # The default limit is 20,000 rows.
    attr_reader :in_memory_limit

    def initialize(limit = 200_000)
      @in_memory_limit = limit
      @mutex = Mutex.new
      reset
    end

    # Resets the data, total rows etc back to 0
    def reset
      @mutex.synchronize do
        @total_rows = 0
        @max_row_size = 0
        @data = []
      end
    end

    # Add a single row to the in memory dataset. Will
    # automatically stop once the limit is reached.
    #
    # @param [Array] row
    def <<(row)
      raise ArgumentError, 'Row must be an array' unless row.is_a?(Array)

      @mutex.synchronize do
        @data << row unless @data.size >= @in_memory_limit
        @total_rows += 1
        @max_row_size = [@max_row_size, row.to_s.bytesize].max
      end
    end

    def add_row(row)
      self << row
    end

    # Returns the streamed result set thus far upto the
    # specified limit or default 20,000 rows.
    def data
      @mutex.synchronize { @data }
    end

    # The total rows streamed. This is everything written to the IO object
    # including whats in memory.
    def total_rows
      @mutex.synchronize { @total_rows }
    end

    # Largest row in bytesize. Can estimate eventual file size.
    def max_row_size
      @mutex.synchronize { @max_row_size }
    end
  end
end
