require 'time'

module DWH
  module Adapters
    # TokenManageable hold the logic to load, store and delete tokens from the token store.
    module TokenManageable
      def token_store
        config[:token_store]
      end

      def load_tokens_from_store
        return nil unless token_store.respond_to?(:load)

        payload = token_store.load
        normalize_token_payload(payload)
      rescue StandardError => e
        logger.warn("Failed loading token from token_store: #{e.message}")
        nil
      end

      def store_tokens_in_store(token_payload)
        return unless token_store.respond_to?(:store)

        token_store.store(normalize_token_payload_for_store(token_payload))
      rescue StandardError => e
        logger.warn("Failed storing token in token_store: #{e.message}")
      end

      def delete_tokens_from_store
        return unless token_store.respond_to?(:delete)

        token_store.delete
      rescue StandardError => e
        logger.warn("Failed deleting token from token_store: #{e.message}")
      end

      private

      def normalize_token_payload(payload)
        return nil unless payload.is_a?(Hash)

        data = payload.transform_keys(&:to_sym)
        access_token = data[:access_token]
        access_token = nil if access_token.to_s.strip == ''

        refresh_token = data[:refresh_token]
        refresh_token = nil if refresh_token.respond_to?(:empty?) && refresh_token.empty?
        return nil if access_token.nil? && refresh_token.nil?

        {
          access_token: access_token&.to_s,
          refresh_token: refresh_token,
          expires_at: parse_token_expiry(data[:expires_at])
        }
      end

      def normalize_token_payload_for_store(payload)
        data = payload.transform_keys(&:to_sym)
        cleaned = {
          access_token: data[:access_token]&.to_s,
          refresh_token: data[:refresh_token],
          expires_at: parse_token_expiry(data[:expires_at]),
          token_type: data[:token_type],
          scope: data[:scope],
          issued_at: parse_token_expiry(data[:issued_at]),
          raw: data[:raw]
        }
        cleaned.reject { |_k, v| v.nil? }
      end

      def parse_token_expiry(value)
        return value if value.is_a?(Time)
        return nil if value.nil? || value.to_s.strip == ''

        Time.parse(value.to_s)
      rescue StandardError
        nil
      end
    end
  end
end
