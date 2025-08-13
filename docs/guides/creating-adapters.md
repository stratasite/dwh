<!--
# @title Creating Custom Adapters
-->
# Creating Custom Adapters

The whole point of this library is to make adding a new database integration easy.  With a few steps you can create your own adapter.  If its generic, please contribute back to the project via PR.

This guide walks you through creating your own custom database adapter for DWH. Creating a new adapter involves extending the base adapter class, implementing required methods, and optionally creating custom settings.

## Understanding DWH Architecture

DWH adapters have a simple, focused architecture:

- **5 Core Methods**: Every adapter must implement 5 essential methods
- **YAML Settings**: Database-specific behavior controlled by YAML configuration
- **Configuration Validation**: Automatic validation of connection parameters
- **Function Translation**: SQL functions automatically translated to database-specific syntax

## Minimal Adapter Example

Here's a minimal adapter implementation:

```ruby
module DWH
  module Adapters
    class MyCustomAdapter < Adapter
      # Define required configuration parameters
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: false, default: 1234, message: 'port to connect to'
      config :database, String, required: true, message: 'name of database to connect to'
      config :username, String, required: true, message: 'connection username'
      config :password, String, required: false, default: nil, message: 'connection password'

      # Implement required methods
      def connection
        # Return your database connection object
        # This is cached, so implement connection reuse here
        @connection ||= create_connection
      end

      def tables(catalog: nil, schema: nil)
        # Return array of DWH::Table objects
        # Use catalog/schema for filtering if supported
      end

      def metadata(table_name, catalog: nil, schema: nil)
        # Return single DWH::Table object with column information
      end

      def stats(table_name, date_column: nil, catalog: nil, schema: nil)
        # Return DWH::TableStats object with row counts and date ranges
      end

      def execute(sql, format: :array, retries: 0)
        # Execute SQL and return results in specified format
        # Formats: :array, :object, :csv, :native
      end

      def execute_stream(sql, io, stats: nil)
        # Execute SQL and stream results directly to IO object
      end

      def stream(sql, &block)
        # Execute SQL and yield chunks to block
      end

      private

      def create_connection
        # Your database-specific connection logic
        MyDatabaseClient.connect(
          host: config[:host],
          port: config[:port],
          database: config[:database],
          username: config[:username],
          password: config[:password]
        )
      end
    end
  end
end

# Register your adapter
DWH.register(:mycustom, DWH::Adapters::MyCustomAdapter)
```

## Step-by-Step Implementation

### 1. Define Configuration Parameters

Use the `config` class method to define connection parameters:

```ruby
class MyCustomAdapter < Adapter
  # Required parameters
  config :host, String, required: true, message: 'server host ip address or domain name'
  config :database, String, required: true, message: 'name of database to connect to'
  
  # Optional parameters with defaults
  config :port, Integer, required: false, default: 5432, message: 'port to connect to'
  config :timeout, Integer, required: false, default: 30, message: 'connection timeout'
  
  # Boolean parameters
  config :ssl, Boolean, required: false, default: false, message: 'use ssl connection'
  
  # Parameters with allowed values
  config :auth_type, String, required: false, default: 'basic', 
         message: 'authentication type', allowed: %w[basic oauth token]
end
```

### 2. Implement Connection Management

```ruby
def connection
  return @connection if @connection && connection_valid?

  @connection = create_connection
end

private

def create_connection
  # Example for HTTP-based database
  Faraday.new(
    url: "#{protocol}://#{config[:host]}:#{config[:port]}",
    headers: build_headers,
    request: {
      timeout: config[:timeout]
    }
  )
end

def build_headers
  headers = { 'Content-Type' => 'application/json' }
  headers['Authorization'] = "Bearer #{config[:token]}" if config[:token]
  headers
end

def connection_valid?
  # Implement connection health check
  @connection&.get('/health')&.success?
rescue
  false
end
```

### 3. Implement Table Discovery

```ruby
def tables(catalog: nil, schema: nil)
  query = build_tables_query(catalog: catalog, schema: schema)
  results = execute(query, format: :array)
  
  results.map do |row|
    DWH::Table.new(
      physical_name: row[0],
      schema: row[1] || 'default',
      catalog: row[2],
      table_type: row[3] || 'TABLE'
    )
  end
end

private

def build_tables_query(catalog: nil, schema: nil)
  query = "SHOW TABLES"
  
  conditions = []
  conditions << "FROM #{catalog}" if catalog
  conditions << "LIKE '#{schema}.%'" if schema
  
  query += " #{conditions.join(' ')}" unless conditions.empty?
  query
end
```

### 4. Implement Metadata Extraction

```ruby
def metadata(table_name, catalog: nil, schema: nil)
  # Parse table name if it includes schema/catalog
  parsed = parse_table_name(table_name, catalog: catalog, schema: schema)
  
  query = build_describe_query(parsed[:table], parsed[:schema], parsed[:catalog])
  results = execute(query, format: :array)
  
  columns = results.map do |row|
    DWH::Column.new(
      name: row[0],
      data_type: row[1],
      normalized_data_type: normalize_data_type(row[1]),
      nullable: row[2] != 'NO',
      default_value: row[3],
      character_maximum_length: row[4],
      numeric_precision: row[5],
      numeric_scale: row[6]
    )
  end
  
  DWH::Table.new(
    physical_name: parsed[:table],
    schema: parsed[:schema],
    catalog: parsed[:catalog],
    columns: columns
  )
end
```

### 5. Implement Statistics Collection

```ruby
def stats(table_name, date_column: nil, catalog: nil, schema: nil)
  parsed = parse_table_name(table_name, catalog: catalog, schema: schema)
  full_table_name = build_full_table_name(parsed)
  
  # Get row count
  count_query = "SELECT COUNT(*) FROM #{full_table_name}"
  row_count = execute(count_query, format: :array).first.first
  
  # Get date range if date column provided
  date_start = date_end = nil
  if date_column
    date_query = "SELECT MIN(#{date_column}), MAX(#{date_column}) FROM #{full_table_name}"
    date_result = execute(date_query, format: :array).first
    date_start, date_end = date_result
  end
  
  DWH::TableStats.new(
    row_count: row_count,
    date_start: date_start,
    date_end: date_end
  )
end
```

### 6. Implement Query Execution

```ruby
def execute(sql, format: :array, retries: 0)
  response = connection.post('/query', { sql: sql }.to_json)
  
  raise DWH::ExecutionError, "Query failed: #{response.body}" unless response.success?
  
  raw_data = JSON.parse(response.body)
  format_results(raw_data, format)
rescue => e
  if retries > 0
    sleep(1)
    execute(sql, format: format, retries: retries - 1)
  else
    raise DWH::ExecutionError, "Query execution failed: #{e.message}"
  end
end

def execute_stream(sql, io, stats: nil)
  # For HTTP APIs, you might need to paginate or use streaming endpoints
  offset = 0
  limit = 10_000
  
  loop do
    paginated_sql = "#{sql} LIMIT #{limit} OFFSET #{offset}"
    results = execute(paginated_sql, format: :array)
    
    break if results.empty?
    
    results.each do |row|
      csv_row = CSV.generate_line(row)
      io.write(csv_row)
      stats&.add_row(row)
    end
    
    offset += limit
  end
end

def stream(sql, &block)
  # Similar to execute_stream but yields chunks to block
  offset = 0
  limit = 10_000
  
  loop do
    paginated_sql = "#{sql} LIMIT #{limit} OFFSET #{offset}"
    chunk = execute(paginated_sql, format: :array)
    
    break if chunk.empty?
    
    yield chunk
    offset += limit
  end
end

private

def format_results(raw_data, format)
  case format
  when :array
    raw_data['rows']
  when :object
    columns = raw_data['columns']
    raw_data['rows'].map { |row| columns.zip(row).to_h }
  when :csv
    CSV.generate do |csv|
      raw_data['rows'].each { |row| csv << row }
    end
  when :native
    raw_data
  else
    raise ArgumentError, "Unsupported format: #{format}"
  end
end
```

## Creating Custom Settings

### 1. Create Settings File

Create by copying the [base settings file](https://github.com/stratasite/dwh/blob/main/lib/dwh/settings/base.yml) to a relative directory like so:`settings/mycustom.yml`

```yaml
# Override base settings for your database

# Function mappings
truncate_date: "DATE_TRUNC('@unit', @exp)"
date_literal: "DATE('@val')"
cast: "CAST(@exp AS @type)"

# String functions
trim: "LTRIM(RTRIM(@exp))"
upper_case: "UPPER(@exp)"
lower_case: "LOWER(@exp)"

# Null handling
if_null: "ISNULL(@exp, @when_null)"
null_if: "CASE WHEN @exp = @target THEN NULL ELSE @exp END"

# Capabilities
supports_window_functions: true
supports_array_functions: false
supports_common_table_expressions: true
supports_temp_tables: false

# Query behavior
temp_table_type: "subquery"  # options: cte, subquery, temp
final_pass_measure_join_type: "inner"  # inner, left, right, full

# Custom settings for your database
custom_query_prefix: "/* Generated by DWH */"
max_query_length: 1000000
```

### 2. Custom Settings Location

```ruby
class MyCustomAdapter < Adapter
  # Specify custom settings file location
  settings_file_path "/path/to/my_custom_settings.yml"
  
  # ... rest of implementation
end
```

## Advanced Features

### Error Handling

```ruby
def execute(sql, format: :array, retries: 0)
  # Your execution logic
rescue MyDatabaseClient::ConnectionError => e
  raise DWH::ConnectionError, "Database connection failed: #{e.message}"
rescue MyDatabaseClient::QueryError => e
  raise DWH::ExecutionError, "Query execution failed: #{e.message}"
rescue => e
  raise DWH::AdapterError, "Unexpected error: #{e.message}"
end
```

### Custom Function Translation

```ruby
def custom_function(expression, param1, param2)
  # Access settings for function templates
  template = settings[:custom_function] || "CUSTOM_FUNC(@exp, @p1, @p2)"
  
  template.gsub('@exp', expression)
          .gsub('@p1', param1.to_s)
          .gsub('@p2', param2.to_s)
end
```

## Registration and Usage

### Register Your Adapter

```ruby
# In your gem or application initialization
require 'dwh'
require 'my_custom_adapter'

DWH.register(:mycustom, DWH::Adapters::MyCustomAdapter)
```

### Use Your Adapter

```ruby
# Create adapter instance
adapter = DWH.create(:mycustom, {
  host: 'database.example.com',
  port: 1234,
  database: 'analytics',
  username: 'analyst',
  password: 'secret'
})

# Use standard DWH interface
tables = adapter.tables
metadata = adapter.metadata('users')
results = adapter.execute("SELECT COUNT(*) FROM users")
```

## Examples to Study

Look at existing adapters for implementation patterns:

- **PostgreSQL** (`lib/dwh/adapters/postgres.rb`) - RDBMS with full SQL support
- **Druid** (`lib/dwh/adapters/druid.rb`) - HTTP API-based adapter
- **DuckDB** (`lib/dwh/adapters/duck_db.rb`) - Embedded database adapter

