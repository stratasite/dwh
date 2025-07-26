module DWH
  # TableStats is returned when calling the stat method
  # on an adapter. Its currently just basic stat providerd
  # but could be enhanced in the future to provide more 
  # introspection on the table. For examples, indexes and
  # partition settings. Cardinality of various columns
  # etc.
  class TableStats
    attr_accessor :row_count, :date_start, :date_end

    def initialize(row_count: ,date_start:, date_end:)
      @row_count = row_count
      @date_start = date_start.is_a?(String) ? DateTime.parse(date_start) : date_start
      @date_end = date_end.is_a?(String) ? DateTime.parse(date_end) : date_end
    end
  end
end
