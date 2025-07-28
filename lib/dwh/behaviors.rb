module DWH
  # The Behaviors module will help us write SQL queries that are
  # optimized for the target database.  These are setup primarily for
  # the purposes of Strata.  However, any sql writer can use this to
  # write better sql.
  #
  # For exmaple temp_table_type will tell us what the preferred temporary
  # table strategy should be.  intermediate_measure_filter? will let us know
  # if an aggregation should be filtered in a CTE, final pass, or both.
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
    # just the first one.
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
      %w[both intermediate].include?(settings[:cross_universe_measure_filtering_strategy])
    end

    def final_measure_filter?
      %w[both final].include?(settings[:cross_universe_measure_filtering_strategy])
    end
  end
end
