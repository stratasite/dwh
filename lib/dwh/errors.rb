module DWH
  # Top level Error class for lib.
  class DWHError < StandardError; end

  # ConfigError catches issues related to how an
  # adapter was configured and instantiated.
  class ConfigError < DWHError; end

  # ExecutionError are thrown when there is a failuire
  # to execute calls against the remote db server.
  class ExecutionError < DWHError; end

  # Connection erros are thrown when we fail to
  # obtain a connection for the target database.
  class ConnectionError < DWHError; end

  # UnspportedCapability are thrown when calling a function
  # that the target database does not support.
  class UnsupportedCapability < StandardError; end

  # Handle errors related to OAuth
  class OAuthError < StandardError; end

  # Handle Token Expirattion
  class TokenExpiredError < OAuthError; end

  # Hangle auth errors
  class AuthenticationError < OAuthError; end
end
