module DWH
  module Functions
    # Manages translations to Null handling Functions
    module Nulls
      # If the provided expression is null
      # then return the second expression.
      # @param exp [String] - target expression
      # @param exp_if_null [String] - exp if target is null
      def if_null(exp, exp_if_null)
        gsk(:if_null)
          .sub(/@exp/i, exp.to_s)
          .sub(/@when_null/i, exp_if_null.to_s)
      end

      # Returns Null if the given expression
      # equals the target expression.
      def null_if(exp, target_exp)
        gsk(:null_if)
          .sub(/@exp/i, exp.to_s)
          .sub(/@target/i, target_exp.to_s)
      end

      # Returns Null if the given expression
      # evaluates to zero.
      def null_if_zero(exp)
        gsk(:null_if_zero)
          .sub(/@exp/i, exp.to_s)
      end
    end
  end
end
