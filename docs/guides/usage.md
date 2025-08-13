<!--
# @title Advanced Usage
-->
# Advanced Usage Guide

This guide covers advanced features and usage patterns for DWH, including function translation, streaming, connection pooling, and performance optimization.

## SQL Function Translation

DWH provides automatic translation of common SQL functions to database-specific syntax. This allows you to write database-agnostic code while leveraging native optimizations.

### Date Functions

#### Date Truncation

```ruby
# Truncate dates to different periods
adapter.truncate_date('week', 'created_at')
# PostgreSQL: DATE_TRUNC('week', created_at)
# SQL Server: DATETRUNC(week, created_at)
# MySQL: DATE_FORMAT(created_at, '%Y-%m-%d') - complex logic

adapter.truncate_date('month', 'order_date')
adapter.truncate_date('year', 'signup_date')
```

#### Date Literals

```ruby
# Create database-specific date literals
adapter.date_literal('2025-01-01')
# PostgreSQL: '2025-01-01'::DATE
# SQL Server: '2025-01-01'
# MySQL: '2025-01-01'

adapter.date_time_literal('2025-01-01 10:30:00')
# PostgreSQL: '2025-01-01 10:30:00'::TIMESTAMP
# SQL Server: '2025-01-01 10:30:00'
```

#### Date Arithmetic

```ruby
# Add/subtract time periods
adapter.date_add('created_at', 30, 'day')
# PostgreSQL: (created_at + '30 day'::interval)
# SQL Server: DATEADD(day, 30, created_at)
# MySQL: TIMESTAMPADD(day, 30, created_at)

adapter.date_diff('end_date', 'start_date', 'day')
# Calculate difference between dates in specified units
```

#### Date Extraction

```ruby
# Extract date parts
adapter.extract_year('created_at')
# PostgreSQL: extract(year from created_at)
# SQL Server: DATEPART(year, created_at)

adapter.extract_month('created_at')
adapter.extract_day_of_week('created_at')
adapter.extract_quarter('created_at')
```

### String Functions

```ruby
# String manipulation
adapter.trim('column_name')        # Remove whitespace
adapter.upper_case('column_name')  # Convert to uppercase
adapter.lower_case('column_name')  # Convert to lowercase

# Quoting and literals
adapter.quote('column_name')       # Database-specific column quoting
adapter.string_literal('value')    # Database-specific string literals
```

### Null Handling

```ruby
# Null value handling
adapter.if_null('column1', "'default'")
# PostgreSQL: COALESCE(column1, 'default')
# SQL Server: ISNULL(column1, 'default')

adapter.null_if('column1', "'empty'")
# Returns NULL if column1 equals 'empty'

adapter.null_if_zero('numeric_column')
# Returns NULL if numeric_column equals 0
```

### Array Functions

Available for databases that support array operations (PostgreSQL, Druid):

```ruby
# Check if array contains any values from a list
adapter.array_in_list('tags', "'tech', 'science'")
# PostgreSQL: tags && ARRAY['tech', 'science']
# Druid: MV_OVERLAP(tags, ARRAY['tech', 'science'])

# Check if array excludes all values from a list
adapter.array_exclude_list('categories', "'spam', 'test'")

# Unnest/explode array for joins
adapter.array_unnest_join('tags', 'tag_alias')
# PostgreSQL: CROSS JOIN UNNEST(tags) AS tag_alias
# Druid: CROSS JOIN UNNEST(MV_TO_ARRAY(tags)) tag_alias
```

### Type Casting

```ruby
# Database-specific type casting
adapter.cast('column_name', 'INTEGER')
# PostgreSQL: column_name::INTEGER
# SQL Server: CAST(column_name AS INTEGER)
# MySQL: CAST(column_name AS SIGNED)
```

## Streaming and Large Result Sets

### Basic Streaming

```ruby
# Stream results directly to a file
File.open('large_export.csv', 'w') do |file|
  adapter.execute_stream("SELECT * FROM large_table", file)
end

# Stream with custom processing
adapter.stream("SELECT * FROM large_table") do |chunk|
  # Process each chunk as it arrives
  process_data_chunk(chunk)
end
```

### Streaming with Statistics

```ruby
# Create streaming stats collector
stats = DWH::StreamingStats.new(10_000)  # Keep 10k rows in memory for preview

# Stream with stats tracking
File.open('export.csv', 'w') do |file|
  exec_thread = adapter.execute_stream("SELECT * FROM large_table", file, stats: stats)
  
  # Monitor progress in another thread
  Thread.new do
    loop do
      puts "Processed: #{stats.total_rows} rows"
      puts "Preview size: #{stats.data.size} rows"
      puts "Max row size: #{stats.max_row_size} bytes"
      sleep(5)
      break unless exec_thread.alive? 
    end
  end
end

# Access collected statistics
puts "Final count: #{stats.total_rows}"
puts "Sample data: #{stats.data.first(5)}"
```

### Memory Management

```ruby
# Configure streaming stats memory usage
stats = DWH::StreamingStats.new(50_000)  # Keep more data for larger previews

# Reset stats for reuse
stats.reset

# Manual memory management
stats.add_row(['col1', 'col2', 'col3'])
current_data = stats.data  # Thread-safe access
```

## Connection Pooling

### Creating Connection Pools

```ruby
# Create a named connection pool
pool = DWH.pool('analytics_pool', :postgres, {
  host: 'localhost',
  database: 'analytics',
  username: 'analyst',
  password: 'password'
}, size: 10, timeout: 5)

# Multiple pools for different databases
etl_pool = DWH.pool('etl_pool', :postgres, etl_config, size: 5)
reporting_pool = DWH.pool('reporting_pool', :mysql, reporting_config, size: 15)
```

### Using Connection Pools

```ruby
# Basic pool usage
pool.with do |connection|
  results = connection.execute("SELECT COUNT(*) FROM users")
  metadata = connection.metadata('orders')
end

# Nested pool operations
pool.with do |conn1|
  users = conn1.execute("SELECT id FROM users LIMIT 100")
  
  pool.with do |conn2|  # Gets different connection from pool
    orders = conn2.execute("SELECT * FROM orders WHERE user_id IN (?)", 
                          users.map(&:first))
  end
end
```

### Pool Management

```ruby
# Check pool status
puts "Pool size: #{pool.size}"
puts "Available connections: #{pool.available}"
puts "Active connections: #{pool.in_use}"

# Graceful shutdown
DWH.shutdown('analytics_pool')

# Shutdown all pools
DWH.shutdown_all
```

## Database Capabilities Detection

### Checking Capabilities

```ruby
# Check what features are supported
if adapter.supports_window_functions?
  query = "SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) FROM users"
  results = adapter.execute(query)
end
```

### Available Capability Checks

- `supports_table_join?` - Basic JOIN support
- `supports_full_join?` - FULL OUTER JOIN support
- `supports_cross_join?` - CROSS JOIN support
- `supports_sub_queries?` - Subquery support
- `supports_common_table_expressions?` - CTE support
- `supports_temp_tables?` - Temporary table support
- `supports_window_functions?` - Window function support
- `supports_array_functions?` - Array operation support

## Performance Optimization

### Query Timeouts

```ruby
# Set query timeouts per adapter
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  query_timeout: 1800  # 30 minutes
})

# For long-running analytical queries
druid = DWH.create(:druid, {
  host: 'localhost',
  port: 8080,
  protocol: 'http',
  query_timeout: 3600  # 1 hour
})
```

### Result Format Optimization

```ruby
# Choose appropriate result format for your use case
arrays = adapter.execute(sql, format: :array)    # Fastest, least memory
objects = adapter.execute(sql, format: :object)  # Hash access, more memory
csv = adapter.execute(sql, format: :csv)         # String format
native = adapter.execute(sql, format: :native)   # Database's native format
```

### Streaming for Large Results

```ruby
# Use streaming for large result sets
def export_large_table(adapter, table_name, output_file)
  query = "SELECT * FROM #{table_name}"
  
  File.open(output_file, 'w') do |file|
    adapter.execute_stream(query, file)
  end
end

# Chunk processing for memory efficiency
def process_large_dataset(adapter, query)
  adapter.stream(query) do |chunk|
    # Process each chunk immediately
    # Avoids loading entire result set into memory
    chunk.each { |row| process_row(row) }
  end
end
```

## Error Handling and Debugging

### Comprehensive Error Handling

```ruby
begin
  results = adapter.execute("SELECT * FROM table")
rescue DWH::ExecutionError => e
  # Query execution failed
  puts "Query failed: #{e.message}"
  puts "SQL: #{e.sql}" if e.respond_to?(:sql)
rescue DWH::ConnectionError => e
  # Connection issues
  puts "Connection failed: #{e.message}"
  # Implement retry logic
rescue DWH::ConfigError => e
  # Configuration problems
  puts "Configuration error: #{e.message}"
rescue DWH::UnsupportedCapability => e
  # Attempted unsupported operation
  puts "Feature not supported: #{e.message}"
end
```

## Custom Settings and Overrides

### Runtime Settings Modification

```ruby
# Create adapter with custom settings
adapter = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  settings: {
    quote: '`@exp`',  # Use backticks instead of double quotes
    supports_window_functions: false,  # Force disable window functions
    temp_table_type: 'cte'  # Prefer CTEs over subqueries
  }
})

# Modify settings at runtime
adapter.alter_settings({
  supports_full_join: false,  # Disable FULL JOINs
  final_pass_measure_join_type: 'inner'  # Use INNER JOINs
})

# Reset to original settings
adapter.reset_settings
```

### Custom Function Mappings

```ruby
# Override specific function translations
adapter = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  settings: {
    truncate_date: "DATE_TRUNC('@unit', @exp)",  # Custom date truncation
    cast: "@exp::@type",  # PostgreSQL-style casting
    null_if: "CASE WHEN @exp = @target THEN NULL ELSE @exp END"  # Custom NULLIF
  }
})
```

