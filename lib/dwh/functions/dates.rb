module DWH
  module Functions
    # Standard date functions except for those dealing with extracting
    # a date part.
    module Dates
      # Ruby format string that is compatible for the target database.
      def date_format
        settings[:date_format]
      end

      # Ruby format string that is compatible timestamp format
      # for the target db.
      def date_time_format
        settings[:date_time_format]
      end

      # Ruby format string that is compatible timestamp with timezone
      # format for the target db.
      def date_time_tz_format
        settings[:date_time_tz_format]
      end

      # The native date storage type of the db.
      def date_data_type
        settings[:data_type]
      end

      # Whether the db uses an int format store dates natively
      def date_int?
        date_data_type =~ /int/i
      end

      # The native function to return current date
      def current_date
        settings[:current_date]
      end

      # The native function to return current time
      def current_time
        settings[:current_time]
      end

      # The native function to return current timestamp
      def current_timestamp
        settings[:current_timestamp]
      end

      TIMESTAMPABLE_UNITS = %w[millisecond second minute hour].freeze

      # Given a date expression, truncate it to a given level.
      # The SQL output of a truncation is still date or timestamp.
      #
      # @param unit [String] the unit to truncate to
      # @param exp [String] the expression to truncate
      #
      # @example Truncate date to the week start day
      #   truncate_date('week', 'my_date_col')
      #   Postgres ==> DATE_TRUNC('week', 'my_date_col')
      #   SQL Server ==> DATETRUNC(week, 'my_date_col')
      #
      # @note When truncating a literal date rather than an
      #   expression, the date_literal function should be called on
      #   it first. e.g. truncate_date('week', date_lit('2025-08-06'))
      #   For many dbs it won't matter, but some require date literals
      #   to be specified.
      def truncate_date(unit, exp)
        unit = unit.strip.downcase
        res = if unit == 'week' && adjust_week_start_day?
                gsk("#{settings[:week_start_day].downcase}_week_start_day")
                  .gsub(/@exp/i, exp)
              else
                gsk(:truncate_date)
                  .gsub(/@unit/i, unit)
                  .gsub(/@exp/i, exp)

              end

        # If we are truncating above the timestamp level ie days, years etc
        # then we can cast the result to date
        if TIMESTAMPABLE_UNITS.include?(unit)
          res
        else
          cast(res, 'DATE')
        end
      end

      # Add some interval of time to a given date expression.
      # @param unit [String] the units we are adding i.e day, month, week etc
      # @param val [Integer] the number of said units
      # @param exp [String] the target expression being operated on
      def date_add(unit, val, exp)
        gsk(:date_add)
          .gsub(/@unit/i, unit)
          .gsub(/@val/i, val.to_s)
          .gsub(/@exp/i, exp)
      end

      # Differnect between two dates in terms of the given unit.
      # @param unit [String] i.e day, month, hour etc
      # @param start_exp [String] starting date expression
      # @param end_exp [String] ending date expression
      def date_diff(unit, start_exp, end_exp)
        gsk(:date_diff)
          .gsub(/@unit/i, unit)
          .gsub(/@start_exp/i, start_exp)
          .gsub(/@end_exp/i, end_exp)
      end

      # Applies the given format to the target date expression
      def date_format_sql(exp, format)
        gsk(:date_format_sql)
          .gsub(/@exp/i, exp)
          .gsub(/@format/i, format)
      end

      DATE_CLASSES = [Date, DateTime, Time].freeze

      # Generates a valid date literal string. Most db's
      # this is just single quoted value while others require
      # a date declaration.
      # @param val [String, Date, DateTime, Time]
      def date_literal(val)
        val = DATE_CLASSES.include?(val.class) ? val.strftime(date_format) : val
        gsk(:date_literal).gsub(/@val/i, val)
      end

      # @see #date_literal
      def date_lit(val)
        date_literal(val)
      end

      # @param val [String, Date, DateTime, Time]
      def date_time_literal(val)
        val = DATE_CLASSES.include?(val.class) ? val.strftime(date_time_format) : val
        gsk(:date_time_literal).gsub(/@val/i, val)
      end

      # @see #date_time_literal
      def timestamp_lit(val)
        date_time_literal(val)
      end

      # The current default week start day. This is how
      # the db is currently setup.  Should be either monday or sunday
      def default_week_start_day
        gsk(:default_week_start_day)
      end

      # The desired week start day. Could be diff from the db setting.
      def week_start_day
        gsk(:week_start_day)
      end

      # Whether we need to adjust the week start day. If its different
      # from the default, we need to adjust
      def adjust_week_start_day?
        week_start_day.strip != default_week_start_day.strip
      end

      # Apply translation to desired week start day
      def adjust_week_start_day(exp)
        gsk("#{settings[:week_start_day].downcase}_week_start_day")
          .gsub(/@exp/i, exp)
      end

      # Does the week start on sunday?
      def week_starts_on_sunday?
        gsk(:week_start_day) == 'SUNDAY'
      end
    end
  end
end
