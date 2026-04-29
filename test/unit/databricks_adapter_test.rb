require 'test_helper'
require 'digest'

class DatabricksAdapterTest < Minitest::Test
  FakeResponse = Struct.new(:status, :body)
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

  class FakeConnection
    attr_reader :last_post_path, :last_post_body, :closed

    def initialize(post_responses: [], get_responses: {})
      @post_responses = post_responses.dup
      @get_responses = get_responses.transform_values(&:dup)
      @closed = false
    end

    def post(path)
      req = Struct.new(:body).new
      yield req if block_given?
      @last_post_path = path
      @last_post_body = req.body
      @post_responses.shift || raise("No fake POST response left for #{path}")
    end

    def get(path)
      responses = @get_responses[path] || []
      responses.shift || raise("No fake GET response left for #{path}")
    end

    def close
      @closed = true
    end
  end

  class FakeOAuthClient
    attr_reader :request_bodies

    def initialize(response)
      @response = response
      @request_bodies = []
    end

    def post(_path)
      req = Struct.new(:headers, :body).new({}, nil)
      yield req if block_given?
      @request_bodies << req.body
      @response
    end
  end

  class FakeExternalLinkClient
    def initialize(get_responses: {})
      @get_responses = get_responses.transform_values(&:dup)
    end

    def get(url)
      responses = @get_responses[url] || []
      responses.shift || raise("No fake external-link GET response left for #{url}")
    end
  end

  def setup
    @adapter = build_adapter
  end

  def test_execute_handles_async_poll_and_chunk_fetch
    conn = FakeConnection.new(
      post_responses: [
        res(202, { statement_id: 'stmt-1' })
      ],
      get_responses: {
        '/api/2.0/sql/statements/stmt-1' => [
          res(200, {
                statement_id: 'stmt-1',
                status: { state: 'SUCCEEDED' },
                manifest: {
                  schema: { columns: [{ name: 'id' }] },
                  chunks: [{ chunk_index: 0 }, { chunk_index: 1 }]
                },
                result: { data_array: [['1']] }
              })
        ],
        '/api/2.0/sql/statements/stmt-1/result/chunks/1' => [
          res(200, { data_array: [['2']] })
        ]
      }
    )
    stub_connection(conn)

    result = @adapter.execute('select 1', format: :array)
    payload = JSON.parse(conn.last_post_body)

    assert_equal [['1'], ['2']], result
    assert_equal '/api/2.0/sql/statements', conn.last_post_path
    assert_equal 'INLINE', payload['disposition']
    assert_equal 'JSON_ARRAY', payload['format']
  end

  def test_execute_stream_handles_external_links_csv
    conn = external_links_csv_connection
    adapter = build_adapter(result_disposition: 'EXTERNAL_LINKS', result_format: 'CSV')
    adapter.define_singleton_method(:connection) { conn }
    external_client = external_links_csv_client

    adapter.stub(:external_link_http_client, external_client) do
      io = StringIO.new
      stats = DWH::StreamingStats.new
      adapter.execute_stream('select * from things', io, stats: stats)
      assert_external_links_csv_result(conn, io, stats)
    end
  end

  def test_execute_ignores_result_delivery_overrides_and_uses_inline_json_array
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-override-1',
              status: { state: 'SUCCEEDED' },
              manifest: {
                schema: { columns: [{ name: 'id' }] },
                chunks: [{ chunk_index: 0 }]
              },
              result: { data_array: [['1']] }
            })
      ]
    )
    adapter = build_adapter(result_disposition: 'EXTERNAL_LINKS', result_format: 'CSV')
    adapter.define_singleton_method(:connection) { conn }

    assert_equal [['1']], adapter.execute('select 1', format: :array)
    payload = JSON.parse(conn.last_post_body)
    assert_equal 'INLINE', payload['disposition']
    assert_equal 'JSON_ARRAY', payload['format']
  end

  def test_execute_stream_ignores_result_delivery_overrides_and_uses_external_links_csv
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-override-2',
              status: { state: 'SUCCEEDED' },
              result: {
                external_links: [{
                  chunk_index: 0,
                  external_link: 'https://external.example/chunk-0'
                }]
              }
            })
      ]
    )
    adapter = build_adapter(result_disposition: 'INLINE', result_format: 'JSON_ARRAY')
    adapter.define_singleton_method(:connection) { conn }
    external_client = FakeExternalLinkClient.new(
      get_responses: {
        'https://external.example/chunk-0' => [FakeResponse.new(200, "id,name\n1,alpha\n")]
      }
    )

    adapter.stub(:external_link_http_client, external_client) do
      io = StringIO.new
      stats = DWH::StreamingStats.new
      adapter.execute_stream('select * from things', io, stats: stats)
      payload = JSON.parse(conn.last_post_body)
      assert_equal "id,name\n1,alpha\n", io.string
      assert_equal 'EXTERNAL_LINKS', payload['disposition']
      assert_equal 'CSV', payload['format']
      assert_equal 1, stats.total_rows
    end
  end

  def test_execute_stream_external_links_does_not_count_header_row
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-ext-3',
              status: { state: 'SUCCEEDED' },
              result: {
                external_links: [{
                  chunk_index: 0,
                  external_link: 'https://external.example/chunk-0'
                }]
              }
            })
      ]
    )
    adapter = build_adapter
    adapter.define_singleton_method(:connection) { conn }
    external_client = FakeExternalLinkClient.new(
      get_responses: {
        'https://external.example/chunk-0' => [FakeResponse.new(200, "id,name\n")]
      }
    )

    adapter.stub(:external_link_http_client, external_client) do
      io = StringIO.new
      stats = DWH::StreamingStats.new
      adapter.execute_stream('select * from things', io, stats: stats)
      assert_equal 0, stats.total_rows
    end
  end

  def test_execute_raises_when_external_links_used_without_stream_io
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-ext-2',
              status: { state: 'SUCCEEDED' },
              result: {
                external_links: [{
                  chunk_index: 0,
                  external_link: 'https://external.example/chunk-0'
                }]
              }
            })
      ]
    )
    adapter = build_adapter(result_disposition: 'EXTERNAL_LINKS', result_format: 'CSV')
    adapter.define_singleton_method(:connection) { conn }

    error = assert_raises(DWH::UnsupportedCapability) do
      adapter.execute('select * from things', format: :array)
    end

    assert_match(/execute_stream/, error.message)
  end

  def test_execute_stream_writes_csv_and_stats
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-2',
              status: { state: 'SUCCEEDED' },
              manifest: {
                schema: { columns: [{ name: 'id' }, { name: 'name' }] },
                chunks: [{ chunk_index: 0 }]
              },
              result: { data_array: [%w[1 alpha], %w[2 beta]] }
            })
      ]
    )
    stub_connection(conn)
    io = StringIO.new
    stats = DWH::StreamingStats.new

    returned = @adapter.execute_stream('select * from things', io, stats: stats)

    assert_same io, returned
    assert_match(/id,name/, returned.string)
    assert_match(/1,alpha/, returned.string)
    assert_equal 2, stats.total_rows
  end

  def test_stream_yields_rows
    conn = FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-3',
              status: { state: 'SUCCEEDED' },
              manifest: {
                schema: { columns: [{ name: 'id' }] },
                chunks: [{ chunk_index: 0 }]
              },
              result: { data_array: [['1'], ['2']] }
            })
      ]
    )
    stub_connection(conn)
    rows = []

    @adapter.stream('select id from things') { rows << it }

    assert_equal [['1'], ['2']], rows
  end

  def test_tables_uses_information_schema
    @adapter.stub(:execute, [['users'], ['orders']]) do
      assert_equal %w[users orders], @adapter.tables(schema: 'default')
    end
  end

  def test_metadata_builds_table_columns
    rows = [
      %w[id bigint 19 0 nil],
      %w[name string nil nil 255]
    ]
    @adapter.stub(:execute, rows) do
      md = @adapter.metadata('users')
      assert_equal 2, md.columns.size
      assert_equal 'id', md.columns[0].name
      assert_equal 'bigint', md.columns[0].data_type
      assert_equal 'name', md.columns[1].name
    end
  end

  def test_stats_returns_row_count_and_date_range
    @adapter.stub(:execute, [[42, '2025-01-01', '2025-01-31']]) do
      stats = @adapter.stats('users', date_column: 'created_at')
      assert_equal 42, stats.row_count
      assert_equal Date.parse('2025-01-01'), stats.date_start
      assert_equal Date.parse('2025-01-31'), stats.date_end
    end
  end

  def test_test_connection_returns_false_on_failure
    @adapter.stub(:execute, proc { raise StandardError, 'boom' }) do
      assert_equal false, @adapter.test_connection
      assert_raises(DWH::ConnectionError) { @adapter.test_connection(raise_exception: true) }
    end
  end

  def test_close_closes_underlying_connection
    conn = FakeConnection.new
    @adapter.instance_variable_set(:@connection, conn)

    @adapter.close

    assert conn.closed
    assert_nil @adapter.instance_variable_get(:@connection)
  end

  def test_oauth_access_token_uses_cached_token_from_store
    store = TokenStore.new({
                             access_token: 'cached-token',
                             expires_at: Time.now + 300
                           })
    adapter = build_adapter(token_store: store)

    token = adapter.oauth_access_token

    assert_equal 'cached-token', token
  end

  def test_m2m_defaults_remain_without_oauth_redirect_uri
    adapter = build_adapter

    assert_equal false, adapter.send(:oauth_supports_authorization_code_flow?)
    assert_equal true, adapter.send(:oauth_supports_client_credentials_flow?)
    assert_equal false, adapter.send(:oauth_redirect_uri_required?)
    assert_equal false, adapter.send(:oauth_uses_pkce?)
  end

  def test_u2m_flow_is_enabled_when_oauth_redirect_uri_is_configured
    adapter = build_adapter(auth_mode: 'oauth_u2m', oauth_redirect_uri: 'http://localhost:8787/callback')

    assert_equal true, adapter.send(:oauth_supports_authorization_code_flow?)
    assert_equal false, adapter.send(:oauth_supports_client_credentials_flow?)
    assert_equal true, adapter.send(:oauth_redirect_uri_required?)
    assert_equal true, adapter.send(:oauth_uses_pkce?)
  end

  def test_u2m_authorization_url_includes_expected_core_parameters
    adapter = build_adapter(auth_mode: 'oauth_u2m', oauth_redirect_uri: 'http://localhost:8787/callback')
    url = adapter.authorization_url(state: 'abc123')
    uri = URI.parse(url)
    params = URI.decode_www_form(uri.query).to_h

    assert_equal 'https', uri.scheme
    assert_equal 'workspace.cloud.databricks.com', uri.host
    assert_equal '/oidc/v1/authorize', uri.path
    assert_equal 'code', params['response_type']
    assert_equal 'client-id', params['client_id']
    assert_equal 'http://localhost:8787/callback', params['redirect_uri']
    assert_equal 'abc123', params['state']
  end

  def test_u2m_authorization_url_includes_pkce_challenge
    adapter = build_adapter(auth_mode: 'oauth_u2m', oauth_redirect_uri: 'http://localhost:8787/callback')
    adapter.stub(:oauth_pkce_code_verifier, 'pkce-verifier') do
      url = adapter.authorization_url(state: 'abc123')
      params = URI.decode_www_form(URI.parse(url).query).to_h

      assert_equal 'S256', params['code_challenge_method']
      assert_equal Base64.urlsafe_encode64(Digest::SHA256.digest('pkce-verifier'), padding: false), params['code_challenge']
    end
  end

  def test_u2m_token_exchange_includes_code_verifier
    adapter = build_adapter(auth_mode: 'oauth_u2m', oauth_redirect_uri: 'http://localhost:8787/callback')
    adapter.instance_variable_set(:@oauth_pkce_code_verifier_for_session, 'pkce-verifier')
    oauth_response = FakeResponse.new(200, JSON.generate({
                                                           access_token: 'u2m-token',
                                                           refresh_token: 'u2m-refresh',
                                                           expires_in: 1800,
                                                           token_type: 'Bearer'
                                                         }))
    fake_client = FakeOAuthClient.new(oauth_response)

    adapter.stub(:oauth_http_client, fake_client) do
      adapter.generate_oauth_tokens('auth-code-1')
    end

    params = URI.decode_www_form(fake_client.request_bodies.first).to_h
    assert_equal 'authorization_code', params['grant_type']
    assert_equal 'auth-code-1', params['code']
    assert_equal 'http://localhost:8787/callback', params['redirect_uri']
    assert_equal 'pkce-verifier', params['code_verifier']
  end

  def test_oauth_access_token_mints_and_stores_when_store_is_empty
    store = TokenStore.new(nil)
    adapter = build_adapter(token_store: store)

    oauth_response = FakeResponse.new(200, JSON.generate({
                                                           access_token: 'minted-token',
                                                           expires_in: 3600,
                                                           token_type: 'Bearer'
                                                         }))

    adapter.stub(:oauth_http_client, FakeOAuthClient.new(oauth_response)) do
      token = adapter.oauth_access_token
      assert_equal 'minted-token', token
    end

    refute_nil store.stored
    assert_equal 'minted-token', store.stored[:access_token]
    assert store.stored[:expires_at].is_a?(Time)
  end

  def test_missing_auth_mode_raises_configuration_error
    error = assert_raises(DWH::ConfigError) do
      DWH.create(:databricks, {
                   host: 'workspace.cloud.databricks.com',
                   warehouse: 'warehouse_123',
                   oauth_client_id: 'client-id',
                   oauth_client_secret: 'client-secret',
                   catalog: 'main',
                   schema: 'default'
                 })
    end
    assert_match(/auth_mode/, error.message)
  end

  def test_invalid_auth_mode_raises_configuration_error
    error = assert_raises(DWH::ConfigError) do
      build_adapter(auth_mode: 'oauth')
    end
    assert_match(/Only allowed/, error.message)
  end

  def test_u2m_requires_oauth_redirect_uri
    error = assert_raises(DWH::ConfigError) do
      build_adapter(auth_mode: 'oauth_u2m', oauth_redirect_uri: nil).validate_oauth_config
    end
    assert_match(/oauth_redirect_uri/, error.message)
  end

  def test_oauth_access_token_with_invalid_expiry_falls_back_to_mint
    store = TokenStore.new({
                             access_token: 'bad-cache',
                             expires_at: 'not-a-time'
                           })
    adapter = build_adapter(token_store: store)

    oauth_response = FakeResponse.new(200, JSON.generate({
                                                           access_token: 'fresh-token',
                                                           expires_in: 1800
                                                         }))

    adapter.stub(:oauth_http_client, FakeOAuthClient.new(oauth_response)) do
      token = adapter.oauth_access_token
      assert_equal 'fresh-token', token
    end
  end

  def test_oauth_access_token_with_missing_access_in_store_falls_back_to_mint
    store = TokenStore.new({
                             refresh_token: 'unused',
                             expires_at: Time.now + 500
                           })
    adapter = build_adapter(token_store: store)

    oauth_response = FakeResponse.new(200, JSON.generate({
                                                           access_token: 'fallback-token',
                                                           expires_in: 1800
                                                         }))

    adapter.stub(:oauth_http_client, FakeOAuthClient.new(oauth_response)) do
      token = adapter.oauth_access_token
      assert_equal 'fallback-token', token
    end
  end

  def test_oauth_access_token_refreshes_before_expiry_leeway
    store = TokenStore.new({
                             access_token: 'near-expiry-token',
                             expires_at: Time.now + 10
                           })
    adapter = build_adapter(token_store: store)

    oauth_response = FakeResponse.new(200, JSON.generate({
                                                           access_token: 'rotated-token',
                                                           expires_in: 1800
                                                         }))

    adapter.stub(:oauth_http_client, FakeOAuthClient.new(oauth_response)) do
      token = adapter.oauth_access_token
      assert_equal 'rotated-token', token
    end
  end

  private

  def build_adapter(overrides = {})
    DWH.create(:databricks, {
      host: 'workspace.cloud.databricks.com',
      auth_mode: 'oauth_m2m',
      warehouse: 'warehouse_123',
      oauth_client_id: 'client-id',
      oauth_client_secret: 'client-secret',
      catalog: 'main',
      schema: 'default'
    }.merge(overrides))
  end

  def stub_connection(conn)
    @adapter.stub(:connection, conn) do
      yield if block_given?
    end
    @adapter.define_singleton_method(:connection) { conn }
  end

  def res(status, body)
    FakeResponse.new(status, JSON.generate(body))
  end

  def external_links_csv_connection
    FakeConnection.new(
      post_responses: [
        res(200, {
              statement_id: 'stmt-ext-1',
              status: { state: 'SUCCEEDED' },
              manifest: {
                format: 'CSV',
                chunks: [{ chunk_index: 0 }]
              },
              result: {
                external_links: [{
                  chunk_index: 0,
                  external_link: 'https://external.example/chunk-0',
                  next_chunk_internal_link: '/api/2.0/sql/statements/stmt-ext-1/result/chunks/1'
                }]
              }
            })
      ],
      get_responses: {
        '/api/2.0/sql/statements/stmt-ext-1/result/chunks/1' => [
          res(200, {
                statement_id: 'stmt-ext-1',
                manifest: { format: 'CSV' },
                result: {
                  external_links: [{
                    chunk_index: 1,
                    external_link: 'https://external.example/chunk-1'
                  }]
                }
              })
        ]
      }
    )
  end

  def external_links_csv_client
    FakeExternalLinkClient.new(
      get_responses: {
        'https://external.example/chunk-0' => [FakeResponse.new(200, "id,name\n1,alpha\n")],
        'https://external.example/chunk-1' => [FakeResponse.new(200, "2,beta\n")]
      }
    )
  end

  def assert_external_links_csv_result(conn, io, stats)
    payload = JSON.parse(conn.last_post_body)
    assert_equal "id,name\n1,alpha\n2,beta\n", io.string
    assert_equal 'EXTERNAL_LINKS', payload['disposition']
    assert_equal 'CSV', payload['format']
    assert_equal 2, stats.total_rows
  end
end
