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
                  .gsub('@EXP', exp)
              else
                gsk(:truncate_date)
                  .gsub('@UNIT', unit)
                  .gsub('@EXP', exp)

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
          .gsub('@UNIT', unit)
          .gsub('@VAL', val.to_s)
          .gsub('@EXP', exp)
      end

      # Differnect between two dates in terms of the given unit.
      # @param unit [String] i.e day, month, hour etc
      # @param start_exp [String] starting date expression
      # @param end_exp [String] ending date expression
      def date_diff(unit, start_exp, end_exp)
        gsk(:date_diff)
          .gsub('@UNIT', unit)
          .gsub('@START_EXP', start_exp)
          .gsub('@END_EXP', end_exp)
      end

      # Applies the given format to the target date expression
      def date_format_sql(exp, format)
        gsk(:date_format_sql)
          .gsub('@EXP', exp)
          .gsub('@FORMAT', format)
      end

      # Generates a valid date literal string. Most db's
      # this is just single quoted value while others require
      # a date declaration.
      # @param val [String] should be an actual formated date string.
      def date_literal(val)
        gsk(:date_literal)
          .gsub('@VAL', val)
      end

      def date_time_literal(val)
        gsk(:date_time_literal).gsub('@VAL', val)
      end

      # Converts a Ruby Date into SQL compatible
      # literal value. If a string is passed it will parse
      # the date into Ruby date then traslate it to a
      # valid date
      # @param date [Date, String]
      def date_to_date_literal(date)
        date = Date.parse(date) if date.is_a?(String)
        date_literal(date.strftime(date_format))
      end

      # Converts a Ruby Date into SQL compatible
      # timestamp literal value.
      # @param timestamp [DateTime, String]
      def timestamp_to_timestamp_literal(timestamp)
        timestamp = DateTime.parse(timestamp) if timestamp.is_a?(String)
        date_time_literal(timestamp.strftime(date_time_format))
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
          .gsub('@EXP', exp)
      end

      # Does the week start on sunday?
      def week_starts_on_sunday?
        gsk(:week_start_day) == 'SUNDAY'
      end
    end
  end
end
