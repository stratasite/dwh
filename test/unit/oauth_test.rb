require 'test_helper'
require 'json'

class OAuthTest < Minitest::Test
  class TestAdapter < DWH::Adapters::Adapter
    include DWH::Adapters::OpenAuthorizable

    oauth_with authorize: 'https://example.com/oauth/authorize',
               tokenize: 'https://example.com/oauth/token',
               default_scope: 'read write'

    config :database, String, required: true
    config :oauth_client_id, String, required: false, message: 'OAuth client_id'
    config :oauth_client_secret, String, required: false, message: 'OAuth client_secret'
    config :oauth_redirect_uri, String, required: false, message: 'OAuth redirect_uri'
    config :oauth_scope, String, required: false, message: 'OAuth scope'

    def execute_stream(_sql, _io, stats:)
      5.times do |i|
        stats << [i, 0, 0]
      end
    end
  end

  def setup
    TestAdapter.load_settings
    @adapter = TestAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_client_id',
      oauth_client_secret: 'test_client_secret',
      oauth_redirect_uri: 'https://example.com/callback'
    )
  end

  def test_oauth_endpoints_configuration
    endpoints = @adapter.oauth_settings
    assert_equal 'https://example.com/oauth/authorize', endpoints[:authorize]
    assert_equal 'https://example.com/oauth/token', endpoints[:tokenize]
    assert_equal 'read write', endpoints[:default_scope]
  end

  def test_authorization_url_generation
    url = @adapter.authorization_url(state: 'test_state', scope: 'custom_scope')
    uri = URI.parse(url)
    params = URI.decode_www_form(uri.query).to_h

    assert_equal 'https', uri.scheme
    assert_equal 'example.com', uri.host
    assert_equal '/oauth/authorize', uri.path
    assert_equal 'code', params['response_type']
    assert_equal 'test_client_id', params['client_id']
    assert_equal 'https://example.com/callback', params['redirect_uri']
    assert_equal 'test_state', params['state']
    assert_equal 'custom_scope', params['scope']
  end

  def test_authorization_url_with_default_scope
    @adapter.instance_variable_set(:@config, @adapter.config.merge(oauth_scope: 'default_user_scope'))
    url = @adapter.authorization_url
    params = URI.decode_www_form(URI.parse(url).query).to_h
    assert_equal 'default_user_scope', params['scope']
  end

  def test_apply_oauth_tokens
    @adapter.apply_oauth_tokens(access_token: 'test_access_token', refresh_token: 'test_refresh_token')

    token_info = @adapter.oauth_token_info
    assert_equal 'test_access_token', token_info[:access_token]
    assert_equal 'test_refresh_token', token_info[:refresh_token]
  end

  def test_oauth_authenticated_with_valid_token
    @adapter.apply_oauth_tokens(access_token: 'valid_token', expires_at: Time.now + 3600)

    assert @adapter.oauth_authenticated?
  end

  def test_oauth_authenticated_without_token
    refute @adapter.oauth_authenticated?
  end

  def test_oauth_token_info_structure
    @adapter.apply_oauth_tokens(access_token: 'test_token', refresh_token: 'refresh_token')

    token_info = @adapter.oauth_token_info
    expected_keys = %i[access_token refresh_token expires_at expired authenticated]

    expected_keys.each do |key|
      assert token_info.key?(key), "Token info missing key: #{key}"
    end
  end

  def test_validate_oauth_config_with_missing_client_id
    adapter = TestAdapter.new(database: 'test_db')

    error = assert_raises(DWH::ConfigError) do
      adapter.validate_oauth_config
    end
    assert_match(/oauth_client_id/, error.message)
  end

  def test_validate_oauth_config_with_missing_client_secret
    adapter = TestAdapter.new(database: 'test_db', oauth_client_id: 'test_id')

    error = assert_raises(DWH::ConfigError) do
      adapter.validate_oauth_config
    end
    assert_match(/oauth_client_secret/, error.message)
  end

  def test_refresh_access_token_without_refresh_token
    error = assert_raises(DWH::AuthenticationError) do
      @adapter.refresh_access_token
    end
    assert_match(/No refresh token available/, error.message)
  end

  # Create a test adapter class with proc-based endpoints
  class TestAdapterProc < DWH::Adapters::Adapter
    include DWH::Adapters::OpenAuthorizable

    oauth_with authorize: ->(adapter) { "https://#{adapter.config[:database]}.example.com/oauth/authorize" },
               tokenize: ->(adapter) { "https://#{adapter.config[:database]}.example.com/oauth/token" },
               default_scope: 'dynamic_scope'

    config :database, String, required: true
    config :oauth_client_id, String, required: false
    config :oauth_client_secret, String, required: false
    config :oauth_redirect_uri, String, required: false

    def execute_stream(_sql, _io, stats:); end
  end

  def test_oauth_endpoints_with_proc_configuration
    TestAdapterProc.load_settings
    adapter = TestAdapterProc.new(
      database: 'mydatabase',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret',
      oauth_redirect_uri: 'https://example.com/callback'
    )

    endpoints = adapter.oauth_settings

    # Verify that the procs were called with the adapter instance and generated dynamic URLs
    assert_equal 'https://mydatabase.example.com/oauth/authorize', endpoints[:authorize]
    assert_equal 'https://mydatabase.example.com/oauth/token', endpoints[:tokenize]
    assert_equal 'dynamic_scope', endpoints[:default_scope]
  end
end
