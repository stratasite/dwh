require_relative "functions/dates"
require_relative "functions/extract_date_part"
require_relative "functions/nulls"

module DWH
  module Functions
    include Dates
    include ExtractDatePart
    include Nulls

    def trim(exp)
      gsk(:trim).gsub("@EXP", exp)
    end

    def lower_case(exp)
      gsk(:lower_case).gsub("@EXP", exp)
    end

    def upper_case(exp)
      gsk(:upper_case).gsub("@EXP", exp)
    end

    # Generates sql test to see if any values from the passed
    # in list is in the array column/exp.
    #
    # @param exp [String] - sql expression
    # @param list [String] - comma separated list
    def array_in_list(exp, list)
      raise UnsupportedCapability unless supports_array_functions?
      gsk(:array_in_list).gsub("@EXP", exp).gsub("@LIST", list)
    end

    # Generates sql test to see if any values from the passed
    # in list is NOT in the array column/exp.
    #
    # @param exp [String] - sql expression
    # @param list [String] - comma separated list
    def array_exclude_list(exp, list)
      raise UnsupportedCapability unless supports_array_functions?
      gsk(:array_exclude_list).gsub("@EXP", exp).gsub("@LIST", list)
    end

    # Applies adapter specific quotes around the given
    # expression. The expression is usually a column name, alias,
    # or table alias.
    #
    # @param exp [String] - column, alias, table name
    # @return [String] - quoted string
    def quote(exp)
      gsk(:quote).sub("@EXP", exp)
    end

    # Applies adapter specific string literal translation around the given
    # expression. The expression is usually a string value.
    #
    # @param exp [String] - some string value
    # @return [String] - single quoted string
    def string_lit(exp)
      gsk(:string_literal).sub("@EXP", exp.gsub("'", "''"))
    end

    # Applies adapter specific cross join expression
    # This takes the target table. Ie if you are joining
    # table a with table b.. You will pass in the table b
    # name expression here.
    #
    # @param relation [String] - table name or table exp
    def cross_join(relation)
      gsk(:cross_join).sub("@RELATION", relation)
    end

    def array_unnest_join(exp, table_alias)
      raise UnsupportedCapability unless supports_array_functions?
      gsk(:array_unnest_join)
        .gsub("@EXP", exp)
        .gsub("@ALIAS", table_alias)
    end

    # Shortcut to get settings value
    # by key.
    # @param key [Symbol]|[String]
    def gsk(key)
      settings[key.to_sym].upcase
    end
  end
end
