module DWH
  module Functions
    module Dates
      def date_format
        settings[:date_format]
      end

      def date_time_format
        settings[:date_time_format]
      end

      def date_time_tz_format
        settings[:date_time_tz_format]
      end

      def date_data_type
        settings[:data_type]
      end

      def date_int?
        date_data_type =~ /int/i
      end

      def current_date
        settings[:current_date]
      end

      def current_time
        settings[:current_time]
      end

      def current_timestamp
        settings[:current_timestamp]
      end

      TIMESTAMPABLE_UNITS = %w[millisecond second minute hour]
      def truncate_date(unit, exp)
        unit = unit.strip.downcase
        res = if unit == 'week' && adjust_week_start_day?
                gsk("#{settings[:week_start_day].downcase}_week_start_day")
                  .gsub('@EXP', exp)
              else
                gsk(:truncate_date)
                  .gsub('@UNIT', unit)
                  .gsub('@EXP', exp)

              end

        # If we are truncate above the timestamp level ie days, years etc
        # then we can cast the result to date
        if unit.in?(TIMESTAMPABLE_UNITS)
          res
        else
          cast(res, 'DATE')
        end
      end

      def cast(exp, type)
        gsk(:cast).gsub('@EXP', exp)
                  .gsub('@TYPE', type)
      end

      def date_add(unit, val, exp)
        gsk(:date_add)
          .gsub('@UNIT', unit)
          .gsub('@VAL', val.to_s)
          .gsub('@EXP', exp)
      end

      def date_diff(unit, start_exp, end_exp)
        gsk(:date_diff)
          .gsub('@UNIT', unit)
          .gsub('@START_EXP', start_exp)
          .gsub('@END_EXP', end_exp)
      end

      def date_format_sql(exp, format)
        gsk(:date_format_sql)
          .gsub('@EXP', exp)
          .gsub('@FORMAT', format)
      end

      def date_literal(val)
        gsk(:date_literal)
          .gsub('@VAL', val)
      end

      def date_time_literal(val)
        gsk(:date_time_literal).gsub('@VAL', val)
      end

      # Converts a Ruby Date into SQL compatible
      # literal value.
      def date_lit(date)
        date = Date.parse(date) if date.is_a?(String)
        date_literal(date.strftime(date_format))
      end

      # Converts a Ruby Date into SQL compatible
      # timestamp literal value.
      def timestamp_lit(date)
        date = DateTime.parse(date) if date.is_a?(String)
        date_time_literal(date.strftime(date_time_format))
      end

      def default_week_start_day
        gsk(:default_week_start_day)
      end

      def week_start_day
        gsk(:week_start_day)
      end

      def adjust_week_start_day?
        week_start_day.strip != default_week_start_day.strip
      end

      def adjust_week_start_day(exp)
        gsk("#{settings[:week_start_day].downcase}_week_start_day")
          .gsub('@EXP', exp)
      end

      def week_starts_on_sunday?
        gsk(:week_start_day) == 'SUNDAY'
      end
    end
  end
end
