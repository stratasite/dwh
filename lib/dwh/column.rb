module DWH
  class Column
    attr_reader :schema_type, :data_type, :name, :precision, :scale, :max_char_length

    def initialize(name:, data_type:, precision: nil, scale: nil, schema_type: nil, max_char_length: nil)
      @name = name.downcase
      @precision = precision
      @scale = scale
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

    DEFAULT_RULES = {"_" => " ", "id" => "ID", "desc" => "Description"}
    def namify(rules = DEFAULT_RULES)
      named = name.clone

      rules.each do |k, v|
        named = named.gsub(/#{k}/, v)
      end

      named.titleize keep_id_suffix: true
    end

    def draco_data_type
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
        if precision.to_i >= 38 && scale.to_i == 0
          "bigint"
        elsif !scale.blank? && scale.to_i > 0
          "decimal"
        else
          "integer"
        end
      else
        "string"
      end
    end

    def to_h
      instance_values
    end

    def to_s
      "<Column:#{name}:#{data_type}>"
    end
  end
end
