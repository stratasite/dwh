<!--
# @title Getting Started 
-->
# Getting Started with DWH

DWH is a lightweight library that provides a unified interface to connect, introspect, and query popular databases. This guide will help you get up and running quickly.

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

## Basic Usage

### Creating Your First Connection

```ruby
require 'dwh'

# Connect to PostgreSQL
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  password: 'password'
})

# Connect to DuckDB (in-memory)
duckdb = DWH.create(:duckdb, {
  database: ':memory:'
})
```

### Your First Query

```ruby
# Execute a simple query
results = postgres.execute("SELECT * FROM users LIMIT 10")

# Results are returned as arrays by default
results.each do |row|
  puts row.inspect
end
```

### Exploring Your Database

```ruby
# List all tables
tables = postgres.tables
puts "Available tables: #{tables.map(&:physical_name)}"

# Get detailed information about a table
table_info = postgres.metadata('users')
puts "Table: #{table_info.physical_name}"
puts "Schema: #{table_info.schema}"
puts "Columns:"
table_info.columns.each do |column|
  puts "  #{column.name} (#{column.normalized_data_type})"
end

# Get table statistics
stats = postgres.stats('users', date_column: 'created_at')
puts "Row count: #{stats.row_count}"
puts "Date range: #{stats.date_start} to #{stats.date_end}"
```

### Different Output Formats

```ruby
# Get results as arrays (default)
array_results = postgres.execute("SELECT id, name FROM users LIMIT 5")

# Get results as hashes/objects
hash_results = postgres.execute("SELECT id, name FROM users LIMIT 5", format: :object)

# Get results as CSV string
csv_results = postgres.execute("SELECT id, name FROM users LIMIT 5", format: :csv)

# Stream large results to a file
postgres.execute_stream("SELECT * FROM large_table", File.open('output.csv', 'w'))
```

### Streaming Large Datasets

```ruby
# stream data while tracting stats and previewing data in a separate thread
stats = DWH::StreamingStats.new(10000) # num of rows to keep in memory for previewing
exec_thread  = Thread.new {
  postgres.execute_stream("SELECT * FROM large_table", File.open('output.csv', 'w'), stats: stats)
}

mon_thread = Thread.new{
  loop do
    break if exec_thread.alive?

    puts stats.data.last
  end
}

[exec_thread, mon_thread].each(&:join)

# Stream with block processing
postgres.stream("SELECT * FROM large_table") do |chunk|
  process_chunk(chunk)
end

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

## Core API

Standardized API across adapters:

connection
: creates a reusuable connection based on config hash passed in

tables(schema: nil, catalog: nil)
: returns a list of tables from the default connection or from the specified schema and catalog

metadata(table_name, schema: nil, catalog: nil)
: provides metadata about a table

stats(table_name, date_column: nil)
: provides table row count and date range

execute(sql, format: :array, retries: 0)
: runs a query and returns in given format

execute_stream(sql, io, stats: nil)
: runs a query and streams it as csv into the given io

## Error Handling

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
