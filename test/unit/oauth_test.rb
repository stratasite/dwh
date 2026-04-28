require 'test_helper'
require 'json'

class OAuthTest < Minitest::Test
  TokenStore = Struct.new(:payload, :stored, :deleted) do
    def load
      payload
    end

    def store(token)
      self.stored = token
    end

    def delete
      self.deleted = true
    end
  end

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
    @adapter = build_adapter
  end

  def build_adapter(overrides = {})
    TestAdapter.new({
      database: 'test_db',
      oauth_client_id: 'test_client_id',
      oauth_client_secret: 'test_client_secret',
      oauth_redirect_uri: 'https://example.com/callback'
    }.merge(overrides))
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

  class TestM2MAdapter < DWH::Adapters::Adapter
    include DWH::Adapters::OpenAuthorizable

    config :database, String, required: true
    config :oauth_client_id, String, required: false
    config :oauth_client_secret, String, required: false

    def execute_stream(_sql, _io, stats:); end

    private

    def oauth_tokenization_url
      'https://example.com/oauth/m2m/token'
    end

    def oauth_supports_authorization_code_flow?
      false
    end

    def oauth_supports_client_credentials_flow?
      true
    end

    def oauth_redirect_uri_required?
      false
    end

    def oauth_client_credentials_params
      {
        grant_type: 'client_credentials',
        scope: 'all-apis'
      }
    end
  end

  class TestPkceAdapter < DWH::Adapters::Adapter
    include DWH::Adapters::OpenAuthorizable

    oauth_with authorize: 'https://example.com/oauth/authorize',
               tokenize: 'https://example.com/oauth/token',
               default_scope: 'openid profile'

    config :database, String, required: true
    config :oauth_client_id, String, required: false
    config :oauth_client_secret, String, required: false
    config :oauth_redirect_uri, String, required: false

    def execute_stream(_sql, _io, stats:); end

    private

    def oauth_uses_pkce?
      true
    end
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

  def test_non_pkce_authorization_url_omits_pkce_parameters
    url = @adapter.authorization_url(state: 'test_state')
    params = URI.decode_www_form(URI.parse(url).query).to_h

    refute params.key?('code_challenge')
    refute params.key?('code_challenge_method')
  end

  def test_pkce_authorization_url_adds_challenge_parameters
    TestPkceAdapter.load_settings
    adapter = TestPkceAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret',
      oauth_redirect_uri: 'https://example.com/callback'
    )
    adapter.stub(:oauth_pkce_code_verifier, 'pkce-verifier') do
      url = adapter.authorization_url(state: 'state-1')
      params = URI.decode_www_form(URI.parse(url).query).to_h

      assert_equal 'S256', params['code_challenge_method']
      assert_equal Base64.urlsafe_encode64(Digest::SHA256.digest('pkce-verifier'), padding: false), params['code_challenge']
    end
  end

  def test_pkce_generate_oauth_tokens_sends_code_verifier
    TestPkceAdapter.load_settings
    adapter = TestPkceAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret',
      oauth_redirect_uri: 'https://example.com/callback'
    )
    adapter.instance_variable_set(:@oauth_pkce_code_verifier, 'pkce-verifier')

    response = Struct.new(:status, :body).new(200, JSON.generate({
      access_token: 'new-token',
      refresh_token: 'new-refresh',
      expires_in: 1800,
      token_type: 'Bearer'
    }))

    seen_verifier = nil
    fake_client = Class.new do
      define_method(:initialize) { |result| @result = result }
      define_method(:post) { |_url| @result }
    end.new(response)

    adapter.stub(:oauth_pkce_token_params, lambda { |verifier|
      seen_verifier = verifier
      { code_verifier: verifier }
    }) do
      adapter.stub(:oauth_http_client, fake_client) do
        adapter.generate_oauth_tokens('auth-code-1')
      end
    end

    assert_equal 'pkce-verifier', seen_verifier
  end

  def test_oauth_access_token_hydrates_from_token_store
    store = TokenStore.new({
      access_token: 'store-token',
      refresh_token: 'store-refresh',
      expires_at: Time.now + 3600
    })
    adapter = build_adapter(token_store: store)

    assert_equal 'store-token', adapter.oauth_access_token
  end

  def test_oauth_token_response_stores_tokens_in_store
    store = TokenStore.new(nil)
    adapter = build_adapter(token_store: store)

    response = Struct.new(:status, :body).new(200, JSON.generate({
      access_token: 'new-token',
      refresh_token: 'new-refresh',
      expires_in: 1800,
      token_type: 'Bearer'
    }))

    adapter.send(:oauth_token_response, response)

    refute_nil store.stored
    assert_equal 'new-token', store.stored[:access_token]
    assert_equal 'new-refresh', store.stored[:refresh_token]
    assert store.stored[:expires_at].is_a?(Time)
  end

  def test_oauth_invalid_grant_deletes_stored_token
    store = TokenStore.new(nil)
    adapter = build_adapter(token_store: store)
    adapter.apply_oauth_tokens(access_token: 'expired', refresh_token: 'refresh', expires_at: Time.now - 10)

    response = Struct.new(:status, :body).new(400, JSON.generate({
      error: 'invalid_grant',
      message: 'refresh token expired'
    }))

    assert_raises(DWH::TokenExpiredError) { adapter.send(:oauth_token_response, response) }
    assert_equal true, store.deleted
  end

  def test_oauth_access_token_mints_for_client_credentials_flow
    TestM2MAdapter.load_settings
    adapter = TestM2MAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret'
    )
    adapter.apply_oauth_tokens(access_token: nil, refresh_token: nil, expires_at: nil)

    response = Struct.new(:status, :body).new(200, JSON.generate({
      access_token: 'm2m-access-token',
      expires_in: 1800,
      token_type: 'Bearer'
    }))

    fake_client = Class.new do
      define_method(:initialize) { |result| @result = result }
      define_method(:post) { |_url| @result }
    end.new(response)

    adapter.stub(:oauth_http_client, fake_client) do
      assert_equal 'm2m-access-token', adapter.oauth_access_token
    end
  end

  def test_oauth_access_token_mints_and_stores_for_client_credentials_flow
    TestM2MAdapter.load_settings
    store = TokenStore.new(nil)
    adapter = TestM2MAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret',
      token_store: store
    )

    response = Struct.new(:status, :body).new(200, JSON.generate({
      access_token: 'm2m-access-token',
      expires_in: 1800,
      token_type: 'Bearer'
    }))

    fake_client = Class.new do
      define_method(:initialize) { |result| @result = result }
      define_method(:post) do |_url|
        req = Struct.new(:headers, :body).new({}, nil)
        yield req if block_given?
        @result
      end
    end.new(response)

    adapter.stub(:oauth_http_client, fake_client) do
      assert_equal 'm2m-access-token', adapter.oauth_access_token
    end

    refute_nil store.stored
    assert_equal 'm2m-access-token', store.stored[:access_token]
    assert store.stored[:expires_at].is_a?(Time)
  end

  def test_oauth_access_token_refreshes_and_updates_store_when_expired
    store = TokenStore.new({
      access_token: 'old-access',
      refresh_token: 'refresh-1',
      expires_at: Time.now - 10
    })
    adapter = build_adapter(token_store: store)

    response = Struct.new(:status, :body).new(200, JSON.generate({
      access_token: 'refreshed-access',
      refresh_token: 'refresh-1',
      expires_in: 1800,
      token_type: 'Bearer'
    }))

    fake_client = Class.new do
      define_method(:initialize) { |result| @result = result }
      define_method(:post) do |_url|
        req = Struct.new(:headers, :body).new({}, nil)
        yield req if block_given?
        @result
      end
    end.new(response)

    adapter.stub(:oauth_http_client, fake_client) do
      assert_equal 'refreshed-access', adapter.oauth_access_token
    end

    refute_nil store.stored
    assert_equal 'refreshed-access', store.stored[:access_token]
    assert_equal 'refresh-1', store.stored[:refresh_token]
    assert store.stored[:expires_at].is_a?(Time)
  end

  def test_validate_oauth_config_without_redirect_uri_for_m2m
    TestM2MAdapter.load_settings
    adapter = TestM2MAdapter.new(
      database: 'test_db',
      oauth_client_id: 'test_id',
      oauth_client_secret: 'test_secret'
    )

    assert adapter.validate_oauth_config
  end
end
