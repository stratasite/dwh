# frozen_string_literal: true

# Core extensions to provide ActiveSupport-like functionality using standard Ruby
module DWH
  module CoreExt
    # Object extensions
    module Object
      def present?
        !blank?
      end

      def blank?
        respond_to?(:empty?) ? !!empty? : !self
      end
    end

    # String extensions
    module String
      def demodulize
        split('::').last || ''
      end

      def titleize(keep_id_suffix: false)
        # Convert snake_case to title case
        result = tr('_', ' ').split(' ').map(&:capitalize).join(' ')

        # Handle ID suffix specially if requested
        if keep_id_suffix
          result.gsub(/\bId\b/, 'ID')
        else
          result
        end
      end
    end
  end
end

# Extend core classes
Object.include(DWH::CoreExt::Object)
String.include(DWH::CoreExt::String)
