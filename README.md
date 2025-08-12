# DWH - Data Warehouse Adapter Library

A light weight library to connect, introspect, and query popular databases over a unified interface.  This gem is intended to be used for analtyical workloads primarily.  The library also provides database specific translations for common functions like `date_trunc`, `date_add` etc.  The function tranlation is not comprehensive. DWH provides good coverage for data handling, and some array handling as well.

*This is not an ORM* nor will it cast types to ruby unless the underlying client does it out of the box.  The goal here is to create an Architecture where new databases can be onboarded quickly.

## Why do we need another database abstraction layer?

Libraries like [Sequel](https://github.com/jeremyevans/sequel) are amazing and comprehensive.  However, its broad coverage also makes it more laborious to add new databases.  Especially, ones with only HTTP endpoints for Ruby.  We seem to be having an explosion of databases recently and a light weight interface will allow us to integrate faster.

The adapter only has 5 core methods (6 including the connection method).  A YAML settings controls how it interacts with a particular db.  It is relatively fast to add a new db. See the [Druid](http://github.com/stratasite/dwh/blob/main/lib/dwh/adapters/druid.rb) implementation as an example. And [here](https://github.com/stratasite/dwh/blob/main/lib/dwh/settings/druid.yml) is its corresponding YAML settings file.

## Features

- **Unified Interface**: Connect to multiple database types using the same API
  - **tables**: list all tables (use schema: and catalog: to filter)
  - **metadata**: return table schema for a specific table
  - **stats**: provide basic stats about the table (row count, date range or records)
  - **execute**: runs a query and returns data in the given format (:array, :object (hash), :csv, :native)
  - **execute_stream**: runs a query and streams the result as csv to the provided io object
  - **stream**: runs a query and yields streaming chunks to the given block
- **SQL Function Translation**: Automatically translates common SQL functions to database-specific syntax
- **Connection Pooling**: Built-in connection pool management for high-performance applications
- **Rich Metadata**: Extract table schemas, column information, and statistics

## Supported Databases

- **PostgreSQL** - Full-featured RDBMS with advanced SQL support
- **MySQL** - Popular open-source database
- **SQL Server** - Microsoft's enterprise database
- **Trino** (formerly Presto) - Distributed SQL query engine
- **Apache Druid** - Real-time analytics database
- **DuckDB** - In-process analytical database

## Integrations Coming Soon

- **Snowflake** - Cloud data warehouse platform
- **Redshift** - AWS data warehouse platform
- **ClickHouse** - High performance analytical db
- **Databricks** - Big data compute engine
- **MotherDuck** - Hosted DuckDB service

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'dwh'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install dwh
```

## Quick Start

### Basic Connection

```ruby
require 'dwh'

# Connect to PostgreSQL
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  password: 'password'
})

# Connect to Druid
druid = DWH.create(:druid, {
  host: 'localhost',
  port: 8080,
  protocol: 'http'
})
```

### Executing Queries

```ruby
# Execute a simple query
results = postgres.execute("SELECT * FROM users LIMIT 10")

# Execute with different return formats
results_as_objects = postgres.execute("SELECT * FROM users", format: :object)
results_as_csv = postgres.execute("SELECT * FROM users", format: :csv)

# Stream large result sets
postgres.execute_stream("SELECT * FROM large_table", File.open('output.csv', 'w'))

# stream data while tracting stats and previewing data in a separate thread
stats = DWH::StreamingStats.new(10000) # num of rows to keep in memory for previewing
postgres.execute_stream("SELECT * FROM large_table", File.open('output.csv', 'w'), stats: stats)

# Stream with block processing
postgres.stream("SELECT * FROM large_table") do |chunk|
  process_chunk(chunk)
end
```

### Database Introspection

```ruby
# List all tables
tables = postgres.tables

# List tables in different schema 
tables = postgres.tables schema: 'pg_catalog'

# Get table metadata
table = postgres.metadata('users')
puts table.physical_name    # => "users"
puts table.schema          # => "public"
puts table.columns.first.name  # => "id"
puts table.columns.first.normalized_data_type # => "integer"

# Get table statistics
stats = postgres.stats('users', date_column: 'created_at')
puts stats.row_count       # => 1000
puts stats.date_start      # => 2023-01-01
puts stats.date_end        # => 2024-01-01
```

## Advanced Usage

### Connection Pooling

```ruby
# Create a connection pool
pool = DWH.pool('my_postgres_pool', :postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  password: 'password'
}, size: 10, timeout: 5)

# Use the pool
pool.with do |connection|
  results = connection.execute("SELECT COUNT(*) FROM users")
end

# Shutdown the pool when done
DWH.shutdown('my_postgres_pool')
```

### Database Functions

DWH provides a function translation layer that converts common SQL functions to database-specific syntax:

```ruby
# Date truncation
postgres.truncate_date('week', 'created_at')  # => DATE_TRUNC('week', created_at)
sqlserver.truncate_date('week', 'created_at') # => DATETRUNC(week, created_at)

# Date literals
postgres.date_literal('2025-01-01')   # => '2025-01-01'::DATE
sqlserver.date_literal('2025-01-01')  # => '2025-01-01'

# Null handling
adapter.coalesce('column1', 'column2', "'default'")  # => COALESCE(column1, column2, 'default')
adapter.null_if('column1', "'empty'")                # => NULLIF(column1, 'empty')

# String functions
adapter.trim('column_name')        # => TRIM(column_name)
adapter.upper_case('column_name')  # => UPPER(column_name)
adapter.lower_case('column_name')  # => LOWER(column_name)
```

### Settings and Capabilities

Check database capabilities before using features:

```ruby
if adapter.supports_window_functions?
  query = "SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) FROM users"
  results = adapter.execute(query)
end

if adapter.supports_array_functions?
  # Use array-specific functions
end
```

### Error Handling

```ruby
begin
  results = adapter.execute("SELECT * FROM non_existent_table")
rescue DWH::ExecutionError => e
  puts "Query failed: #{e.message}"
rescue DWH::ConnectionError => e
  puts "Connection failed: #{e.message}"
rescue DWH::ConfigError => e
  puts "Configuration error: #{e.message}"
end
```

## Configuration

### Database-Specific Configuration

Each adapter supports specific configuration options:

#### PostgreSQL

```ruby
DWH.create(:postgres, {
  host: 'localhost',
  port: 5432,
  database: 'mydb',
  schema: 'public',
  username: 'user',
  password: 'password',
  ssl: true,
  query_timeout: 3600,
  client_name: 'My App',
  extra_connection_params: {
    sslmode: 'require',
    sslcert: '/path/to/cert.pem'
  }
})
```

#### Snowflake

```ruby
DWH.create(:snowflake, {
  account_id: 'my_account',
  warehouse: 'my_warehouse',
  database: 'my_database',
  schema: 'PUBLIC',
  username: 'user',
  password: 'password',
  role: 'my_role',
  query_timeout: 600
})
```

#### Druid

```ruby
DWH.create(:druid, {
  protocol: 'https',
  host: 'druid.example.com',
  port: 443,
  query_timeout: 600,
  basic_auth: 'base64_encoded_credentials',
  extra_connection_params: {
    context: {
      user: 'analyst',
      team: 'data'
    }
  }
})
```

### Custom Settings

You can override adapter behavior by providing custom settings:

```ruby
adapter = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  settings: {
    quote: '`@EXP`',  # Use backticks instead of double quotes
    supports_window_functions: false  # Disable window function support
  }
})
```

## Architecture

### Core Components

- **`DWH::Factory`** - Manages adapter registration and instantiation
- **`DWH::Adapters::Adapter`** - Base class for all database adapters
- **`DWH::Table`** - Represents database table metadata
- **`DWH::Column`** - Represents table column information
- **`DWH::TableStats`** - Contains table statistics and metrics
- **`DWH::Functions`** - SQL function translation layer
- **`DWH::Capabilities`** - Database feature detection

### Extending DWH

Register a custom adapter:

```ruby
class MyCustomAdapter < DWH::Adapters::Adapter
  config :host, String, required: true
  config :port, Integer, default: 1234
  
  def connection
    # Implement connection logic
  end
  
  def execute(sql, format: :array, retries: 0)
    # Implement query execution
  end
  
  # Implement other required methods...
end

# Register the adapter
DWH.register(:mycustom, MyCustomAdapter)

# Use the adapter
adapter = DWH.create(:mycustom, { host: 'localhost' })
```

#### Custom Settings Files

DWH uses YAML settings files to control database behavior, SQL function mapping, and capabilities. Each adapter can have its own settings file that overrides the base settings.

**Settings File Structure:**

By default, DWH looks for settings files in `lib/dwh/settings/` with the pattern `{adapter_name}.yml`. You can specify a custom location:

```ruby
class MyCustomAdapter < DWH::Adapters::Adapter
  # Custom settings file location
  settings_file_path "/path/to/my_custom_settings.yml"
  
  # ... rest of adapter implementation
end
```

**Creating Custom Settings:**

Start by copying `lib/dwh/settings/base.yml` and modify it for your database:

```yaml
# my_custom_settings.yml

# Override SQL function patterns
truncate_date: "DATETRUNC('@unit', @exp)"
date_literal: "DATE('@val')"
cast: "CONVERT(@type, @exp)"

# Override capabilities
supports_window_functions: false
supports_array_functions: true

# Custom function mappings
quote: "`@exp`"
string_literal: "'@exp'"

# Date extraction patterns
extract_year: 'DATEPART(year, @exp)'
extract_month: 'DATEPART(month, @exp)'
extract_day_of_week: 'DATEPART(weekday, @exp)'

# Array operations (if supported)
array_in_list: "@exp IN (@list)"
array_exclude_list: "@exp NOT IN (@list)"

# Join behavior
cross_join: "CROSS JOIN @relation"
supports_full_join: false

# Query generation behavior
temp_table_type: "subquery"  # options: cte, subquery, temp
final_pass_measure_join_type: "inner"
```

**Settings Placeholders:**

Settings use placeholder patterns that get replaced during function calls:

- `@exp` - The expression/column name
- `@val` - The value being used
- `@unit` - Time unit (day, week, month, etc.)
- `@type` - Data type for casting
- `@list` - Comma-separated list
- `@relation` - Table/relation name

**Example Usage:**

```ruby
# With custom settings, these function calls will use your database's syntax
adapter.truncate_date('week', 'created_at')  # => DATETRUNC('week', created_at)
adapter.date_literal('2025-01-01')           # => DATE('2025-01-01')
adapter.cast('column_name', 'INTEGER')       # => CONVERT(INTEGER, column_name)
```

**Runtime Settings Override:**

You can also override settings at runtime:

```ruby
adapter = DWH.create(:mycustom, { 
  host: 'localhost',
  settings: {
    quote: '[@exp]',  # Use square brackets for quoting
    supports_temp_tables: false
  }
})

# Or alter settings after creation
adapter.alter_settings({ temp_table_type: 'cte' })

# Reset to original settings
adapter.reset_settings
```

## Testing

Run the test suite:

```bash
bundle exec rake test
```

Run specific adapter tests:

```bash
bundle exec ruby test/system/rdbms_postgres_test.rb
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt.

To install this gem onto your local machine, run `bundle exec rake install`.

## Contributing

Bug reports and pull requests are welcome on GitHub at <https://github.com/stratasite/dwh>.

## License

This project is available as open source under the terms of the MIT License.

## Version

Current version: 0.1.0

