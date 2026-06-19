module DWH
  # Captures column metadata for a target table.
  class Column
    attr_reader :schema_type, :data_type, :name, :precision, :scale, :max_char_length

    def initialize(name:, data_type:, precision: 0, scale: 0, schema_type: nil, max_char_length: nil)
      @name = name.downcase
      @precision = precision.is_a?(String) ? precision.to_i : precision
      @scale = scale.is_a?(String) ? scale.to_i : scale
      @data_type = data_type&.downcase
      @schema_type = schema_type&.downcase
      @max_char_length = max_char_length
    end

    def dim?
      schema_type == 'dimension'
    end

    def measure?
      schema_type == 'measure'
    end

    DEFAULT_RULES = { /[_+]+/ => ' ', /\s+id$/i => ' ID', /desc/i => 'Description' }.freeze
    def namify(rules = DEFAULT_RULES)
      named = titleize(name)
      rules.each do |k, v|
        named = named.gsub(Regexp.new(k), v)
      end

      named
    end

    def normalized_data_type
      # Strip ClickHouse type wrappers (Nullable(T), LowCardinality(T), Array(T))
      # so the inner type is matched by the rules below.
      inner = unwrap_type(data_type)

      case inner
      when /binary/, 'image'
        'binary'
      when /varchar/, 'string', /text/, /char/, /fixedstring/
        'string'
      when 'date', 'date32'
        'date'
      when /date_time/, /datetime/, 'time', /timestamp/
        'date_time'
      when 'int', 'integer', 'smallint', 'tinyint', /^int8$/, /^int16$/, /^int32$/,
           /^uint8$/, /^uint16$/, /^uint32$/
        'integer'
      when 'bigint', 'bit_int', 'big_integer', /^int64$/, /^int128$/, /^int256$/,
           /^uint64$/, /^uint128$/, /^uint256$/
        'bigint'
      when 'decimal', 'double', 'float', 'real', 'dec', 'numeric', 'money',
           /^float32$/, /^float64$/, /^decimal/
        'decimal'
      when 'boolean', 'bit', 'bool'
        'boolean'
      when 'uuid'
        'string'
      when 'number'
        if precision >= 38 && scale.zero?
          'bigint'
        elsif scale.positive?
          'decimal'
        else
          'integer'
        end
      else
        'string'
      end
    end

    def to_h
      {
        name: name,
        data_type: data_type,
        precision: precision,
        scale: scale,
        schema_type: schema_type,
        max_char_length: max_char_length
      }
    end

    def to_s
      "<Column:#{name}:#{data_type}>"
    end

    # Strips ClickHouse parameterized wrappers like Nullable(T), LowCardinality(T),
    # Array(T) so the inner type can be normalised by the standard rules above.
    # Safe to call on any type string; returns the input unchanged if no wrapper matches.
    def unwrap_type(type)
      inner = type.to_s.downcase
      inner = inner.sub(/\Anullable\((.+)\)\z/, '\1')
      inner = inner.sub(/\Alowcardinality\((.+)\)\z/, '\1')
      inner.sub(/\Aarray\((.+)\)\z/, '\1')
    end

    def titleize(name)
      # Handle underscores, dashes, and multiple spaces
      # Also preserves existing spacing patterns better
      name.gsub(/[_-]/, ' ')           # Convert underscores and dashes to spaces
          .gsub(/\s+/, ' ')            # Normalize multiple spaces to single spaces
          .strip                       # Remove leading/trailing whitespace
          .split(' ')                  # Split into words
          .map(&:capitalize)           # Capitalize each word
          .join(' ')                   # Join with single spaces
    end
  end
end
