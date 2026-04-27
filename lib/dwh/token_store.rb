module DWH
  # Optional contract for host applications that want token persistence.
  #
  # The store instance should be identity-bound before it is passed into
  # adapter config so the adapter remains unaware of user/datasource identity.
  #
  # This class is intentionally minimal and can be subclassed or duck-typed.
  class TokenStore
    # @return [Hash,nil] token payload
    def load
      raise NotImplementedError, "#{self.class} must implement ##{__method__}"
    end

    # @param token [Hash] normalized payload with at least access_token and expires_at
    def store(_token)
      raise NotImplementedError, "#{self.class} must implement ##{__method__}"
    end

    # Remove/revoke persisted token state.
    def delete
      raise NotImplementedError, "#{self.class} must implement ##{__method__}"
    end
  end
end
