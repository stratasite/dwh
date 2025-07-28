module DWH
  # This module will handle database features that one might be
  # interested in. Not every database necessarily supports
  # all ANSI SQL features.  This minimal at this point and focused
  # on analytical Query use cases for Strata [https://www.strata.site]
  module Capabilities
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
