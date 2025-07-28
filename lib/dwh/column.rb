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
      schema_type == "dimension"
    end

    def measure?
      schema_type == "measure"
    end

    DEFAULT_RULES = {/[_+]+/ => " ", /\s+id$/i => " ID", /desc/i => "Description"}.freeze
    def namify(rules = DEFAULT_RULES)
      named = name.titleize keep_id_suffix: true
      rules.each do |k, v|
        named = named.gsub(Regexp.new(k), v)
      end

      named
    end

    def normalized_data_type
      case data_type
      when "varchar", "string", "text", "char", "varbinary"
        "string"
      when "date"
        "date"
      when "date_time", "datetime", "timestamp", "time"
        "date_time"
      when "int", "integer", "smallint", "tinyint"
        "integer"
      when "bigint", "bit_int", "big_integer"
        "bigint"
      when "decimal", "double", "float", "real", "dec", "numeric"
        "decimal"
      when "boolean"
        "boolean"
      when "number"
        if precision >= 38 && scale.zero?
          "bigint"
        elsif scale.positive?
          "decimal"
        else
          "integer"
        end
      else
        "string"
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
  end
end
