module DWH
  # This module will handle database features that one might be
  # interested in. Not every database necessarily supports
  # all ANSI SQL features.  This minimal at this point and focused
  # on analytical Query use cases for Strata [https://www.strata.site]
  module Capabilities
    # Returns the full reserved-keyword set for this adapter instance.
    def reserved_keywords
      self.class.reserved_keywords
    end

    # Returns the full aggregate-function set for this adapter instance.
    def aggregate_functions
      self.class.aggregate_functions
    end

    # Is the given identifier a reserved keyword for this adapter?
    def reserved?(name)
      reserved_keywords.include?(name.to_s.downcase)
    end

    # Is the given function name an aggregate for this adapter?
    def aggregate_function?(name)
      aggregate_functions.include?(name.to_s.downcase)
    end

    def supports_table_join?
      settings[:supports_table_join]
    end

    def supports_full_join?
      settings[:supports_full_join]
    end

    def supports_cross_join?
      settings[:supports_cross_join]
    end

    def supports_sub_queries?
      settings[:supports_sub_queries]
    end

    def supports_common_table_expressions?
      settings[:supports_common_table_expressions]
    end

    def supports_temp_tables?
      settings[:supports_temp_tables]
    end

    def supports_window_functions?
      settings[:supports_window_functions]
    end

    def supports_array_functions?
      settings[:supports_array_functions]
    end
  end
end
