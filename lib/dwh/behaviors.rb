module DWH
  module Behaviors
    # In druid when you do specific time range you need
    # to apply the last hour of the day to the date value
    # to get all inclusive data for that date.
    def extend_ending_date_to_last_hour_of_day?
      settings[:extend_ending_date_to_last_hour_of_day]
    end

    # Defines how intermediate queries should be handled.
    # Could be one off cte, subquery, temp (future view, permanent)
    def temp_table_type
      settings[:temp_table_type]
    end

    def temp_table_prefix
      settings[:temp_table_prefix]
    end

    # When an array dimension is projected and it is filtered
    # in the where clause, some db's like Druid needs a
    # having clause to ensure the projected set matches
    # the filtered set.
    def apply_advanced_filtering_on_array_projections?
      settings[:apply_advanced_filtering_on_array_projections]
    end

    # When measures from multiple fact universes are combined
    # they need to be merged in a Merge stage SQL statement.
    # The components is typically combined by Full Outer Join
    # but could be modified as needed.
    def final_pass_measure_join_type
      settings[:final_pass_measure_join_type]
    end

    # When a filter on a Date time field can be applied to multiple
    # tables in the join tree should we apply to all of them or
    # just the first one. (First one in Draco has the highest cardinality.)
    def greedy_apply_date_filters
      settings[:greedy_apply_date_filters]
    end

    # Whether to apply a measure filter to intermediate stages and
    # final pass when appropriate.  This is the case of multi universe
    # query with measure filter. Default behavior is to apply the filter
    # in the intermediate stage and final pass.
    def cross_universe_measure_filtering_strategy
      settings[:cross_universe_measure_filtering_strategy]
    end

    def intermediate_measure_filter?
      settings[:cross_universe_measure_filtering_strategy] == "both" ||
        settings[:cross_universe_measure_filtering_strategy] == "intermediate"
    end

    def final_measure_filter?
      settings[:cross_universe_measure_filtering_strategy] == "both" ||
        settings[:cross_universe_measure_filtering_strategy] == "final"
    end
  end
end
