module DWH
  module Functions
    # Operations on array columns. Mostly used in where clauses
    module Arrays
      # Generates sql test to see if any values from the passed
      # in list is in the array column/exp.
      #
      # @param exp [String] - sql expression
      # @param list [String] - comma separated list
      # @raise UnsupportedCapability if the db doesn't support array functions.
      def array_in_list(exp, list)
        raise UnsupportedCapability unless supports_array_functions?

        gsk(:array_in_list).gsub(/@exp/i, exp).gsub(/@list/i, list)
      end

      # Generates sql test to see if any values from the passed
      # in list is NOT in the array column/exp.
      #
      # @param exp [String] - sql expression
      # @param list [String] - comma separated list
      # @raise UnsupportedCapability if the db doesn't support array functions.
      def array_exclude_list(exp, list)
        raise UnsupportedCapability unless supports_array_functions?

        gsk(:array_exclude_list).gsub(/@exp/i, exp).gsub(/@list/i, list)
      end

      # Explode an array. This should be used in a join expression.
      # @param exp [String] the column/expression that will be Explode
      # @param table_alias [String] the alias of the exploded array
      # @raise UnsupportedCapability if the db doesn't support array functions.
      def array_unnest_join(exp, table_alias)
        raise UnsupportedCapability unless supports_array_functions?

        gsk(:array_unnest_join)
          .gsub(/@exp/i, exp)
          .gsub(/@alias/i, table_alias)
      end
    end
  end
end
