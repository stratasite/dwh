module DWH
  module Functions
    # All date functions related to extracting part of date
    # from a date or timestamp.
    module ExtractDatePart
      def extract_year(exp)
        gsk(:extract_year).gsub(/@exp/i, exp)
      end

      def extract_month(exp)
        gsk(:extract_month).gsub(/@exp/i, exp)
      end

      def extract_quarter(exp)
        gsk(:extract_quarter).gsub(/@exp/i, exp)
      end

      def extract_day_of_year(exp)
        gsk(:extract_day_of_year).gsub(/@exp/i, exp)
      end

      def extract_day_of_month(exp)
        gsk(:extract_day_of_month).gsub(/@exp/i, exp)
      end

      def extract_day_of_week(exp)
        gsk(:extract_day_of_week).gsub(/@exp/i, exp)
      end

      def extract_week_of_year(exp)
        gsk(:extract_week_of_year).gsub(/@exp/i, exp)
      end

      def extract_hour(exp)
        gsk(:extract_hour).gsub(/@exp/i, exp)
      end

      def extract_minute(exp)
        gsk(:extract_minute).gsub(/@exp/i, exp)
      end

      def extract_year_month(exp)
        gsk(:extract_year_month).gsub(/@exp/i, exp)
      end

      def extract_day_name(exp, abbreviate: false)
        upper_case(
          if abbreviate
            date_format_sql(exp,
                            gsk(:abbreviated_day_name_format))
          else
            date_format_sql(exp, gsk(:day_name_format))
          end
        )
      end

      def extract_month_name(exp, abbreviate: false)
        upper_case(
          if abbreviate
            date_format_sql(exp,
                            gsk(:abbreviated_month_name_format))
          else
            date_format_sql(exp,
                            gsk(:month_name_format))
          end
        )
      end
    end
  end
end
