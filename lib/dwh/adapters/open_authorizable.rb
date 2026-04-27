require 'base64'
require 'securerandom'
require_relative 'token_manageable'

module DWH
  module Adapters
    # OpenAuthorizable aka OAuth module will add functionality
    # to get and refresh access tokens for databases that supported
    # OAuth.
    #
    # To use this module include it in your adapter and call the oauth_with
    # class method.
    #
    # @example Endpoint that needs to use instance to generate
    #   oauth_with authorize: ->(adapter) { "url#{config[:val]}"}, tokenize: "http://blue.com"
    #
    # @example Get authorization_url
    #   adapter.authorization_url
    #   Then capture the code and gen tokens
    #
    # @example Generate acess tokens
    #   adapter.generate_oauth_tokens(code_from_authorization)
    #   # this will also apply the tokens
    #
    # @example Reuse cached tokens
    #   adapter.apply_oauth_tokens(access_token: 'myaccesstoken', refresh_token: 'rtoken', expires_at: Time.now)
    module OpenAuthorizable
      # rubcop:disable Style/DocumentationModule
      module ClassMethods
        def oauth_with(authorize: nil, tokenize: nil, default_scope: 'refresh_token')
          @oauth_settings = { authorize: authorize, tokenize: tokenize, default_scope: default_scope }
        end

        def oauth_settings
          raise OAuthError, 'Please configure oauth settings by calling oauth_with class method.' unless @oauth_settings

          @oauth_settings
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.include(TokenManageable)
        base.config :oauth_client_id, String, required: false, message: 'OAuth client_id'
        base.config :oauth_client_secret, String, required: false, message: 'OAuth client_secret'
        base.config :oauth_redirect_uri, String, required: false, message: 'OAuth redirect_uri'
        base.config :oauth_scope, String, required: false, message: 'OAuth scope'
      end

      # Generate authorization URL for user to visit
      def authorization_url(state: SecureRandom.hex(16), scope: nil)
        raise UnsupportedCapability, "#{adapter_name} does not support authorization-code OAuth flow" unless oauth_supports_authorization_code_flow?

        params = {
          'response_type' => 'code',
          'client_id' => oauth_client_id,
          'redirect_uri' => oauth_redirect_uri,
          'state' => state,
          'scope' => scope || oauth_scope || oauth_settings[:default_scope]
        }.compact

        uri = URI(oauth_settings[:authorize])
        uri.query = URI.encode_www_form(params)
        uri.to_s
      end

      # You can reuse existing tokens that were saved outside
      # of this app by passing it here.  This could be tokens
      # cached from a previous call to @see #generate_oauth_tokens
      #
      # param access_token [String] the access token
      # @param refresh_token [String] optional refresh token
      def apply_oauth_tokens(access_token: nil, refresh_token: nil, expires_at: nil)
        @oauth_access_token = access_token
        @oauth_refresh_token = refresh_token
        @token_expires_at = expires_at
      end

      # Takes the given authorization code and generates new
      # access and refresh tokens. It will also apply them.
      # @param authorization_code [String] this code should come from
      #   the redirect that is captured from the #authorization_url
      def generate_oauth_tokens(authorization_code)
        raise UnsupportedCapability, "#{adapter_name} does not support authorization-code OAuth flow" unless oauth_supports_authorization_code_flow?

        params = {
          grant_type: 'authorization_code',
          code: authorization_code,
          redirect_uri: oauth_redirect_uri
        }

        response = oauth_http_client.post(oauth_tokenization_url) do |req|
          req.headers['Content-Type'] = 'application/x-www-form-urlencoded'
          req.headers['Authorization'] = basic_auth_header
          req.body = URI.encode_www_form(params)
        end
        oauth_token_response(response)
      end

      # Refresh access token using refresh token
      def refresh_access_token
        raise AuthenticationError, 'No refresh token available' unless @oauth_refresh_token

        params = oauth_refresh_token_params
        response = oauth_http_client.post(oauth_tokenization_url) do |req|
          req.headers['Content-Type'] = 'application/x-www-form-urlencoded'
          req.headers['Authorization'] = oauth_token_request_auth_header
          req.body = URI.encode_www_form(params)
        end

        oauth_token_response(response)
      end

      def mint_access_token
        raise UnsupportedCapability, "#{adapter_name} does not support client-credentials OAuth flow" unless oauth_supports_client_credentials_flow?

        params = oauth_client_credentials_params
        response = oauth_http_client.post(oauth_tokenization_url) do |req|
          req.headers['Content-Type'] = 'application/x-www-form-urlencoded'
          req.headers['Authorization'] = oauth_token_request_auth_header
          req.body = URI.encode_www_form(params)
        end

        oauth_token_response(response)
      end

      # This will return the current access_token or
      # if it expired and refresh_token token is available
      # it will generate a new token.
      #
      # @return [String] access token
      # @raise [AuthenticationError]
      def oauth_access_token
        load_oauth_tokens_from_store! unless @oauth_access_token || @oauth_refresh_token
        return @oauth_access_token if oauth_token_usable?

        refresh_access_token if oauth_refresh_token_usable?
        return @oauth_access_token if oauth_token_usable?

        mint_access_token if oauth_supports_client_credentials_flow?
        return @oauth_access_token if oauth_token_usable?

        raise AuthenticationError,
              'Access token was never set. Either run the auth flow, mint via client credentials, or set tokens via apply_oauth_tokens.'
      end

      # Check if we have a valid access token
      def oauth_authenticated?
        @oauth_access_token && oauth_token_usable?
      end

      # Get current state of tokens
      def oauth_token_info
        {
          access_token: @oauth_access_token,
          refresh_token: @oauth_refresh_token,
          expires_at: @token_expires_at,
          expired: !oauth_token_usable?,
          authenticated: oauth_authenticated?
        }
      end

      def validate_oauth_config
        raise ConfigError, 'Missing config: oauth_client_id. Required for OAuth.' unless config[:oauth_client_id]
        raise ConfigError, 'Missing config: oauth_client_secret. Required for OAuth.' unless config[:oauth_client_secret]
        if oauth_redirect_uri_required?
          raise ConfigError, 'Missing config: oauth_redirect_uri. Required for OAuth.' unless config[:oauth_redirect_uri]
        end

        oauth_settings if oauth_supports_authorization_code_flow?
        true
      end

      def oauth_settings
        @oauth_settings ||= self.class.oauth_settings.transform_values do
          it.is_a?(Proc) ? it.call(self) : it
        end
      end

      def oauth_tokenization_url
        oauth_settings[:tokenize]
      end

      protected

      def basic_auth_header
        credentials = Base64.strict_encode64("#{config[:oauth_client_id]}:#{config[:oauth_client_secret]}")
        "Basic #{credentials}"
      end

      def oauth_token_request_auth_header
        basic_auth_header
      end

      def oauth_refresh_token_params
        {
          grant_type: 'refresh_token',
          refresh_token: @oauth_refresh_token
        }
      end

      def oauth_client_credentials_params
        {
          grant_type: 'client_credentials',
          scope: oauth_scope || oauth_settings[:default_scope]
        }.compact
      end

      def oauth_supports_authorization_code_flow?
        true
      end

      def oauth_supports_client_credentials_flow?
        false
      end

      def oauth_redirect_uri_required?
        oauth_supports_authorization_code_flow?
      end

      def oauth_token_expiry_leeway_seconds
        0
      end

      def oauth_token_usable?
        return false unless @oauth_access_token

        !token_expiring_soon?
      end

      def oauth_refresh_token_usable?
        @oauth_refresh_token && token_expired?
      end

      def token_expiring_soon?(seconds = oauth_token_expiry_leeway_seconds)
        return true if @token_expires_at.nil?

        (Time.now + seconds) >= @token_expires_at
      end

      def oauth_http_client
        @oauth_http_client ||= Faraday.new(
          headers: {
            'Content-Type' => 'application/json',
            'User-Agent' => config[:client_name]
          }
        )
      end

      # Override this method to handle provider-specific token response formats
      def oauth_token_response(response)
        case response.status
        when 200..299
          data = JSON.parse(response.body)

          apply_oauth_tokens(access_token: data['access_token'],
                             refresh_token: data['refresh_token'] || @oauth_refresh_token)

          # Calculate expiration time
          expires_in = data['expires_in'] || 3600
          @token_expires_at = Time.now + expires_in
          store_tokens_in_store(
            access_token: @oauth_access_token,
            refresh_token: @oauth_refresh_token,
            expires_at: @token_expires_at,
            token_type: data['token_type'],
            scope: data['scope'],
            raw: data
          )

          { success: true, data: data }
        else
          error_data = parse_error_response(response)
          if error_data['error'] == 'invalid_grant' && @oauth_refresh_token
            delete_tokens_from_store
            raise TokenExpiredError, "Potentially expired refresh token. #{error_data['message']}"
          end

          raise AuthenticationError, "Token request failed: #{error_data['error']} - #{error_data['message']}"
        end
      end

      private

      def load_oauth_tokens_from_store!
        payload = load_tokens_from_store
        return unless payload

        apply_oauth_tokens(
          access_token: payload[:access_token],
          refresh_token: payload[:refresh_token],
          expires_at: payload[:expires_at]
        )
      end

      def parse_error_response(response)
        JSON.parse(response.body)
      rescue JSON::ParserError
        { 'error' => 'unknown', 'message' => response.body }
      end
    end
  end
end
