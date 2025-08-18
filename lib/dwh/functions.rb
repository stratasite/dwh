require_relative 'functions/dates'
require_relative 'functions/extract_date_part'
require_relative 'functions/nulls'
require_relative 'functions/arrays'

module DWH
  # The Functions module adds a translation layer between this library
  # and native database functions. It relies on the adapters settings
  # file to map standard function to its native counterpart.
  #
  # @example Truncate date to the week start day
  #   truncate_date('week', 'my_date_col')
  #   Postgres ==> DATE_TRUNC('week', 'my_date_col')
  #   SQL Server ==> DATETRUNC(week, 'my_date_col')
  #
  # @example Output a date string as a valid date literal
  #   date_literal('2025-08-06')
  #   Postgres ==> '2025-08-06'::DATE
  #   SQL Server ==> '2025-08-06'
  module Functions
    include Dates
    include ExtractDatePart
    include Nulls
    include Arrays

    # Casts an expresion/literal to the target datatype.
    # Datatype should be valid for the target db.
    #
    # @param exp [String] sql expression
    # @param type [String] valid type for target db
    def cast(exp, type)
      gsk(:cast).gsub(/@exp/i, exp)
                .gsub(/@type/i, type)
    end

    def trim(exp)
      gsk(:trim).gsub(/@exp/i, exp)
    end

    def lower_case(exp)
      gsk(:lower_case).gsub(/@exp/i, exp)
    end

    def upper_case(exp)
      gsk(:upper_case).gsub(/@exp/i, exp)
    end

    # Applies adapter specific quotes around the given
    # expression. The expression is usually a column name, alias,
    # or table alias.
    #
    # @param exp [String] - column, alias, table name
    def quote(exp)
      gsk(:quote).sub(/@exp/i, exp)
    end

    # Applies adapter specific string literal translation around the given
    # expression. The expression is usually a string value.
    #
    # @param exp [String] some string value
    def string_lit(exp)
      gsk(:string_literal).sub(/@exp/i, exp.gsub("'", "''"))
    end

    # Applies adapter specific cross join expression
    # This takes the target table. Ie if you are joining
    # table a with table b.. You will pass in the table b
    # name expression here.
    #
    # @example
    #   adapter.cross_join("schema.table_b")
    #
    # @param relation [String] - table name or table exp
    def cross_join(relation)
      gsk(:cross_join).sub(/@relation/i, relation)
    end

    # Shortcut to get settings value
    # by key.
    # @param key [Symbol,String]
    # @return [String] upcased value
    def gsk(key)
      settings[key.to_sym]
    end
  end
end
