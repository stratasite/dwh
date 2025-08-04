module DWH
  # TableStats is returned when calling Adapter#stat.
  # This currently ust provide basic stats,
  # but could be enhanced in the future to provide more
  # introspection on the table. For examples, indexes and
  # partition settings. Cardinality of various columns
  # etc.
  class TableStats
    # @return [Integer] total rows in table
    attr_accessor :row_count

    # @return [DateTime] when a date column is passed to {Adapters::Adapter#stats} it
    #   will return date of first record in the table
    attr_accessor :date_start

    # @return [DateTime] when a date column is passed to {Adapters::Adapter#stats} it
    #   returns the date of the last record in the table
    attr_accessor :date_end

    def initialize(row_count: nil, date_start: nil, date_end: nil)
      @row_count = row_count.nil? ? 0 : row_count.to_i
      @date_start = parse_date(date_start)
      @date_end = parse_date(date_end)
    end

    # Hash of the stats attributes
    # @return [Hash] of the attributes
    def to_h
      {
        row_count: row_count,
        date_start: date_start,
        date_end: date_end
      }
    end

    private

    def parse_date(date)
      case date
      when nil
        date
      when String
        DateTime.parse(date)
      when Time, Date
        DateTime.parse(date.to_s)
      else
        raise ConfigError, "Unexpected date class: #{date.class.name}"
      end
    end
  end
end
