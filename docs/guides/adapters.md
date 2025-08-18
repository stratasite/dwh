<!--
# @title Adapter Configuration
-->
# Adapter Configuration

This guide covers all the database adapters supported by DWH and their specific configuration options. Each adapter is designed to work with specific database clients and provides database-specific optimizations.

## PostgreSQL Adapter

The PostgreSQL adapter uses the `pg` gem and provides full-featured RDBMS support.

### Basic Configuration

```ruby
postgres = DWH.create(:postgres, {
  host: 'localhost',
  port: 5432,                    # Default: 5432
  database: 'mydb',
  schema: 'public',              # Default: 'public'
  username: 'user',
  password: 'password',
  client_name: 'My Application'  # Default: 'DWH Ruby Gem'
})
```

### SSL Configuration

```ruby
# Basic SSL
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  password: 'password',
  ssl: true,
  extra_connection_params: {
    sslmode: 'require'  # disable, prefer, require, verify-ca, verify-full
  }
})

# Certificate-based SSL
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  ssl: true,
  extra_connection_params: {
    sslmode: 'verify-full',
    sslrootcert: '/path/to/ca-cert.pem',
    sslcert: '/path/to/client-cert.pem',
    sslkey: '/path/to/client-key.pem'
  }
})
```

### Advanced Configuration

```ruby
postgres = DWH.create(:postgres, {
  host: 'localhost',
  database: 'mydb',
  username: 'user',
  password: 'password',
  query_timeout: 3600,  # seconds, default: 3600
  extra_connection_params: {
    application_name: 'Data Analysis Tool',
    connect_timeout: 10,
    options: '-c maintenance_work_mem=256MB'
  }
})
```

## MySQL Adapter

The MySQL adapter uses the `mysql2` gem. Note that MySQL's concept of "database" maps to "schema" in DWH.

### Basic Configuration

```ruby
mysql = DWH.create(:mysql, {
  host: '127.0.0.1',            # Use 127.0.0.1 for local Docker instances
  port: 3306,                   # Default: 3306
  database: 'mydb',
  username: 'user',
  password: 'password',
  client_name: 'My Application' # Default: 'DWH Ruby Gem'
})
```

### SSL Configuration

```ruby
# Basic SSL
mysql = DWH.create(:mysql, {
  host: '127.0.0.1',
  database: 'mydb',
  username: 'user',
  password: 'password',
  ssl: true,  # Defaults ssl_mode to 'required'
  extra_connection_params: {
    ssl_mode: 'verify_identity',  # disabled, preferred, required, verify_ca, verify_identity
    sslca: '/path/to/ca-cert.pem',
    sslcert: '/path/to/client-cert.pem',
    sslkey: '/path/to/client-key.pem'
  }
})
```

### Advanced Configuration

```ruby
mysql = DWH.create(:mysql, {
  host: 'mysql.example.com',
  database: 'analytics',
  username: 'analyst',
  password: 'password',
  client_name: "My App", # defaults to 'DWH Ruby Gem'
  query_timeout: 1800,  # seconds, default: 3600
  extra_connection_params: {
    encoding: 'utf8mb4',
    read_timeout: 60,
    write_timeout: 60,
    connect_timeout: 10
  }
})
```

## SQL Server Adapter

The SQL Server adapter uses the `tiny_tds` gem and supports both on-premises and Azure SQL Server.

### Basic Configuration

```ruby
sqlserver = DWH.create(:sqlserver, {
  host: 'localhost',
  port: 1433,                   # Default: 1433
  database: 'mydb',
  username: 'sa',
  password: 'password',
  client_name: 'My Application' # Default: 'DWH Ruby Gem'
})
```

### Azure SQL Server

```ruby
azure_sql = DWH.create(:sqlserver, {
  host: 'myserver.database.windows.net',
  database: 'mydb',
  username: 'myuser@myserver',
  password: 'password',
  azure: true,
  client_name: 'My Application'
})
```

### Advanced Configuration

```ruby
sqlserver = DWH.create(:sqlserver, {
  host: 'sql.example.com',
  database: 'analytics',
  username: 'analyst',
  password: 'password',
  query_timeout: 1800,  # seconds, default: 3600
  extra_connection_params: {
    container: true,     # For SQL Server running in containers
    use_utf16: false,    # Character encoding options
    timeout: 60,         # Connection timeout
    login_timeout: 60    # Login timeout
  }
})
```

### Multi-Database Operations

```ruby
# List tables in another database
tables = sqlserver.tables(catalog: 'other_database')

# Get metadata for table in another database
metadata = sqlserver.metadata('other_database.dbo.my_table')
# OR
metadata = sqlserver.metadata('my_table', catalog: 'other_database')
```

## DuckDB Adapter

The DuckDB adapter uses the `ruby-duckdb` gem for in-process analytical queries. This requires DuckDB header files and library to already be installed.

### Basic Configuration

```ruby
# File-based database
duckdb = DWH.create(:duckdb, {
  file: '/path/to/my/database.duckdb',
  schema: 'main'  # Default: 'main'
})

# In-memory database
duckdb = DWH.create(:duckdb, {
  file: ':memory:'
})
```

### Read-Only Mode

```ruby
duckdb = DWH.create(:duckdb, {
  file: '/path/to/readonly/database.duckdb',
  duck_config: {
    access_mode: 'READ_ONLY'
  }
})
```

### Advanced Configuration

```ruby
duckdb = DWH.create(:duckdb, {
  file: '/path/to/my/database.duckdb',
  duck_config: {
    access_mode: 'READ_WRITE',
    max_memory: '2GB',
    threads: 4,
    temp_directory: '/tmp/duckdb'
  }
})
```

## Trino Adapter

The Trino adapter requires the `trino-client-ruby` gem and works with both Trino and Presto.

### Basic Configuration

```ruby
trino = DWH.create(:trino, {
  host: 'localhost',
  port: 8080,                   # Default: 8080
  catalog: 'hive',              # Required
  schema: 'default',            # Optional
  username: 'analyst',
  password: 'password',         # Optional
  client_name: 'My Application' # Default: 'DWH Ruby Gem'
})
```

### SSL Configuration

```ruby
trino = DWH.create(:trino, {
  host: 'trino.example.com',
  port: 443,
  ssl: true, # will set {ssl: {verify: false}}
  catalog: 'hive',
  username: 'analyst',
  password: 'password',
  client_name: "My App"
})
```

### Advanced Configuration with Headers

```ruby
trino = DWH.create(:trino, {
  host: 'trino.example.com',
  port: 8080,
  catalog: 'delta_lake',
  schema: 'analytics',
  username: 'analyst',
  query_timeout: 1800,  # seconds, default: 3600
  extra_connection_params: {
    http_headers: {
      'X-Trino-User' => 'Real User Name',
      'X-Trino-Source' => 'Analytics Dashboard',
      'X-Forwarded-Request' => 'client-request-id'
    },
    ssl: {
      verify: true,
    }
  }
})
```

## Apache Druid Adapter

The Druid adapter uses HTTP API calls via the `faraday` gem for real-time analytics.

### Basic Configuration

```ruby
druid = DWH.create(:druid, {
  protocol: 'http',             # 'http' or 'https'
  host: 'localhost',
  port: 8080,                   # Default: 8081
  client_name: 'My Application' # Default: 'DWH Ruby Gem'
})
```

### HTTPS with Basic Authentication

```ruby
druid = DWH.create(:druid, {
  protocol: 'https',
  host: 'druid.example.com',
  port: 443,
  basic_auth: 'base64_encoded_credentials',  # Base64 encoded username:password
  query_timeout: 600,          # seconds, default: 600
  open_timeout: 30             # connection timeout, default: nil
})
```

### Advanced Configuration with Context

```ruby
druid = DWH.create(:druid, {
  protocol: 'https',
  host: 'druid.example.com',
  port: 8080,
  basic_auth: 'dXNlcjpwYXNz',  # base64 for 'user:pass'
  extra_connection_params: {
    context: {
      user: 'analyst_name',
      team: 'data_engineering',
      priority: 10,
      useCache: true
    }
  }
})
```

## AWS Athena Adapter

The Athean adapter requires the `aws-athena-sdk` gem and works with both Trino and Presto.

### Basic Configuration

```ruby
athena = DWH.create(:athena, {
    region: 'us-east-1',
    database: 'default',
    s3_output_location: 's3://my-athena-results-bucket/queries/',
    access_key_id: 'AKIAIOSFODNN7EXAMPLE',
    secret_access_key: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    catalog: 'hive',       # optional will default to awsdatacatalog
    database: 'default',   # Optional. Db or schema
    workgroup: 'my-dept-strata' # optional workgroup
})
```

### SSL Configuration

```ruby
athena = DWH.create(:athena, {
    region: 'us-east-1',
    database: 'default',
    s3_output_location: 's3://my-athena-results-bucket/queries/',
    access_key_id: 'AKIAIOSFODNN7EXAMPLE',
    secret_access_key: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    catalog: 'hive',       # optional will default to awsdatacatalog
    database: 'default',   # Optional. Db or schema
    workgroup: 'my-dept-strata', # optional workgroup
    extra_connection_params: {
      ssl_ca_directory: 'path/to/certs/'
    }
})
```

### Advanced Configuration with Headers

See full list of config options here: [athena-api](https://docs.aws.amazon.com/sdk-for-ruby/v2/api/Aws/Athena/Client.html#initialize-instance_method)

## Configuration Validation

DWH validates configuration parameters at creation time:

```ruby
begin
  adapter = DWH.create(:postgres, { host: 'localhost' })  # Missing required database
rescue DWH::ConfigError => e
  puts "Configuration error: #{e.message}"
end
```

Each adapter defines required and optional parameters with validation rules. Check the adapter-specific sections above for the complete list of supported parameters.
