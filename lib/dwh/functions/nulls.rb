module DWH
  module Functions
    module Nulls
      # If the provided expression is null
      # then return the second expression.
      # @param exp [String] - target expression
      # @param exp_if_null [String] - exp if target is null
      def if_null(exp, exp_if_null)
        gsk(:if_null)
          .sub('@EXP', exp.to_s)
          .sub('@WHEN_NULL', exp_if_null.to_s)
      end

      # Returns Null if the given expression
      # equals the target expression.
      def null_if(exp, target_exp)
        gsk(:null_if)
          .sub('@EXP', exp.to_s)
          .sub('@TARGET', target_exp.to_s)
      end

      # Returns Null if the given expression
      # evaluates to zero.
      def null_if_zero(exp)
        gsk(:null_if_zero)
          .sub('@EXP', exp.to_s)
      end
    end
  end
end
