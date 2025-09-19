
[![Ruby](https://github.com/stratasite/dwh/workflows/Ruby/badge.svg)](https://github.com/stratasite/dwh/actions)

# DWH - Data Warehouse Adapter Library

A light weight library to connect, introspect, and query popular databases over a unified interface.  This gem is intended for analtyical workloads.  The library also provides database specific translations for common functions like `date_trunc`, `date_add` etc.  The function tranlation is not comprehensive. But, it does provides good coverage for date handling, and some array handling as well.

> [!NOTE]
> **This is not an ORM** nor will it cast types to ruby unless the underlying client does it out of the box.  The goal here is to create an Architecture where new databases can be onboarded quickly.

## Why do we need another database abstraction layer?

Libraries like [Sequel](https://github.com/jeremyevans/sequel) are amazing and comprehensive.  However, its broad coverage also makes it more laborious to add new databases.  Especially, ones with only HTTP endpoints for Ruby.  We seem to be having an explosion of databases recently and a light weight interface will allow us to integrate faster.

The adapter only has 5 core methods (6 including the connection method).  A YAML settings controls how it interacts with a particular db.  It is relatively fast to add a new db. See the [Druid](http://github.com/stratasite/dwh/blob/main/lib/dwh/adapters/druid.rb) implementation as an example. And [here](https://github.com/stratasite/dwh/blob/main/lib/dwh/settings/druid.yml) is its corresponding YAML settings file.

## Features

- **Unified Interface**: Connect to multiple database types using the same API
- **SQL Function Translation**: Automatically translates common SQL functions to database-specific syntax
- **Connection Pooling**: Built-in connection pool management for high-performance applications
- **Rich Metadata**: Extract table schemas, column information, and statistics

## Supported Databases

- **Snowflake** - High performance cloud warehouse
- **Trino** (formerly Presto) - Distributed SQL query engine
- **Redshift** - AWS data warehouse platform
- **AWS Athena** - AWS big data warehouse
- **Apache Druid** - Real-time analytics database
- **DuckDB** - In-process analytical database
- **PostgreSQL** - Full-featured RDBMS with advanced SQL support
- **MySQL** - Popular open-source database
- **SQL Server** - Microsoft's enterprise database

## Integrations Coming Soon

- **ClickHouse** - High performance analytical db
- **Databricks** - Big data compute engine
- **MotherDuck** - Hosted DuckDB service

## Quick Start

Install it yourself as:

```bash
gem install dwh
```

### Connect and Execute a Basic Query

```ruby
require 'dwh'

# Connect to Druid
druid = DWH.create(:druid, {
  host: 'localhost',
  port: 8080,
  protocol: 'http'
})

# basic query execution
results = druid.execute("SELECT * FROM web_sales", format: :csv)
```

## Core API

Standardized API across adapters:

<dl>
  <dt>connection</dt>
  <dd>Creates a reusuable connection based on config hash passed in</dd>
  <dt>tables(schema: nil, catalog: nil)</dt>
  <dd> returns a list of tables from the default connection or from the specified schema and catalog </dd>
  <dt> metadata(table_name, schema: nil, catalog: nil) </dt>
  <dd> provides metadata about a table </dd>
  <dt>stats(table_name, date_column: nil) </dt>
  <dd> provides table row count and date range </dd>
  <dt> execute(sql, format: :array, retries: 0) </dt>
  <dd> runs a query and returns in given format </dd>
  <dt> execute_stream(sql, io, stats: nil) </dt>
  <dd> runs a query and streams it as csv into the given io </dd>
</dl>

## Tutorials and Guides

- [Getting Started](https://strata.site/dwh/file.getting-started.html)
- [Adapter Configuration](https://strata.site/dwh/file.adapters.html)
- [Creating an Adapter](https://strata.site/dwh/file.creating-adapters.html)
- [Advanced Usage](https://strata.site/dwh/file.usage.html)
- [API](https://strata.site/dwh/DWH.html)

## Testing

Certain databases have to be tested via docker. Those tests will try to launch docker compose services in `test/support/compose*.yml`

Run Unit Tests:

```bash
bundle exec rake test:unit
```

Run tests on RDBMS dbs:

```bash
bundle exec rake test:system:rdbms 
```

Run tests on  druid:

```bash
bundle exec rake test:system:druid 
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
