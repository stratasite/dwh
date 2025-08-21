require 'base64'
require 'securerandom'

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
    module OpenAuthorizable
      class OAuthError < StandardError; end
      class TokenExpiredError < AuthError; end
      class AuthenticationError < OAuthError; end

      def self.included(base)
        base.extend(ClassMethods)
        config :oauth_client_id, String, required: false, message: 'OAuth client_id'
        config :oauth_client_secret, String, required: false, message: 'OAuth client_secret'
        config :oauth_redirect_uri, String, required: false, message: 'OAuth redirect_uri'
        config :oauth_scope, String, required: false, message: 'OAuth redirect_url'
      end

      # rubcop:disable Style/DocumentationModule
      module ClassMethods
        def self.oauth_with(authorize:, tokenize:, default_scope: 'refresh_token')
          @oauth_endpoints = { authorize: authorize, tokenize: tokenize, default_scope: default_scope }
        end

        def self.oauth_endpoints
          raise OAuthError, 'Please configuratoin endpoints by calling oauth class method.' unless @oauth_endpoints

          @oauth_endpoints
        end
      end

      # Generate authorization URL for user to visit
      def authorization_url
        state = SecureRandom.hex(16)

        params = {
          'response_type' => 'code',
          'client_id' => oauth_client_id,
          'redirect_uri' => oauth_redirect_uri,
          'state' => state,
          'scope' => config[:oauth_scope] || oauth_default_scope
        }.compact

        uri = URI(oauth_authorization_endpoint)
        uri.query = URI.encode_www_form(params)
        uri.to_s
      end

      # You can reuse existing tokens that were saved outside
      # of this app by passing it here.  This could be tokens
      # cached from a previous call to @see #generate_oauth_tokens
      # param access_token [String] the access token
      # @param refresh_token [String] optional refresh token
      def apply_oauth_tokens(access_token:, refesh_token: nil)
        @oauth_access_token = access_token
        @oauth_refresh_token = refesh_token
      end

      # Takes the given authorization code and generates new
      # access and refresh tokens. It will also apply them.
      # @param authorization_code [String] this code should come from
      #   the redirect that is captured from the #authorization_url
      def generate_oauth_tokens(authorization_code)
        params = {
          grant_type: 'authorization_code',
          code: authorization_code,
          redirect_uri: config[:oauth_redirect_uri]
        }

        response = oauth_http_client.post(oauth_tokenization_endpoint) do |req|
          req.headers['Content-Type'] = 'application/x-www-form-urlencoded'
          req.headers['Authorization'] = basic_auth_header
          req.body = URI.encode_www_form(params)
        end
        handle_token_response(response)
      end

      # Refresh access token using refresh token
      def refresh_access_token
        raise AuthenticationError, 'No refresh token available' unless @oauth_refresh_token

        params = {
          grant_type: 'refresh_token',
          refresh_token: @oauth_refresh_token
        }

        response = oauth_http_client.post(oauth_tokenization_endpoint) do |req|
          req.headers['Content-Type'] = 'application/x-www-form-urlencoded'
          req.headers['Authorization'] = basic_auth_header
          req.body = URI.encode_www_form(params)
        end

        handle_token_response(response)
      end

      # Check if we have a valid access token
      def oauth_authenticated?
        @oauth_access_token && !token_expired?
      end

      # Get current state of tokens
      def oauth_token_info
        {
          access_token: @oauth_access_token,
          refresh_token: @oauth_refresh_token,
          expires_at: @oauth_token_expires_at,
          expired: oauth_token_expired?,
          authenticated: oauth_authenticated?
        }
      end

      def validate_oauth_config
        raise ConfigError, 'Missing config: client_id. Required for OAuth.' unless config[:client_id]
        raise ConfigError, 'Missing config: client_secret. Required for OAuth.' unless config[:client_secret]
        raise ConfigError, 'Missing config: redirect_url. Required for OAuth.' unless config[:redirect_url]

        oauth_endpoints
      end

      def oauth_endpoints
        @oauth_endpoints ||= self.class.oauth_endpoints.transform_values do
          it.is_a?(Proc) ? it.call(self) : it
        end
      end

      protected

      def basic_auth_header
        credentials = Base64.strict_encode64("#{config[:oauth_client_id]}:#{config[:oauth_client_secret]}")
        "Basic #{credentials}"
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
      def handle_token_response(response)
        case response.status
        when 200..299
          data = JSON.parse(response.body)

          apply_oauth_tokens(access_token: data['access_token'],
                             refresh_token: data['refresh_token'])

          # Calculate expiration time
          expires_in = data['expires_in'] || 3600
          @token_expires_at = Time.now + expires_in

          { success: true, data: data }
        else
          error_data = parse_error_response(response)
          raise AuthenticationError, "Token request failed: #{error_data['error']} - #{error_data['error_description']}"
        end
      end

      def parse_error_response(response)
        JSON.parse(response.body)
      rescue JSON::ParserError
        { 'error' => 'unknown', 'error_description' => response.body }
      end
    end
  end
end
