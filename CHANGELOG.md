## [Unreleased]

## [0.6.2] - 2026-09-30

### Added

- **BigQuery**: `client_email` and `private_key` config accept the service-account keyfile's fields inline, so a server can connect without the JSON file on disk. `keyfile` and Application Default Credentials keep working.

## [0.6.1] - 2026-09-30

### Fixed

- **BigQuery**: `quote` replaces characters BigQuery rejects in column names (parentheses and similar) so planner-generated aliases such as `Month(Post Date)` execute.

## [0.6.0] - 2026-09-28

### Added

- Google BigQuery adapter (`:bigquery`) with dedicated settings and unit/system test coverage. Authenticates with a service-account keyfile or Application Default Credentials; requires the `google-cloud-bigquery` gem.

## [0.5.1] - 2026-08-03

### Fixed

- **Factory#shutdown**: `case pool.class` never matched (every call fell into `else`); undefined `c` NameError when closing live connections; symbol branch called `delete` on the pool instead of the map. Shutdown now matches on the argument, removes the map entry before closing, and tolerates unknown names.
- **Factory#pool**: check-then-set race could orphan a pool under concurrent first-use; creation is now mutex-guarded.
- **Factory#start_reaper**: no longer checks out a connection just to log stats (which created connections and reset idle clocks); Idle/Available labels use `pool.idle` / `pool.available`; iterates a snapshot; rescues `PoolShuttingDownError` per pool so a retired pool cannot kill the reaper thread.
- **DuckDb#close**: use `@connection&.disconnect` so closing an already-closed adapter does not open a new connection.
- **DuckDb.close_all**: close then clear instead of deleting while iterating (which could skip entries).

## [0.5.0] - 2026-06-19

### Added

- ClickHouse adapter with dedicated settings and test coverage for system and adapter behavior.

### Changed

- Added dialect-specific reserved keywords and aggregate functions for ClickHouse, Snowflake, and Databricks expression parsing.

## [0.4.2] - 2026-05-22

### Fixed

- **DuckDB adapter**: Pass connection options with `config:` when opening a database so initialization uses the correct DuckDB API parameter.

## [0.4.1] - 2026-04-29

### Added

- Databricks `execute_stream` support for `EXTERNAL_LINKS` result delivery using CSV downloads

### Changed

- Databricks now uses method-specific result delivery defaults: `execute` uses `INLINE` + `JSON_ARRAY`, and `execute_stream` uses `EXTERNAL_LINKS` + `CSV`.

## [0.4.0] - 2026-04-28

### Added

- Token persistence interface via `DWH::TokenStore` for adapters that support OAuth token lifecycle management.
- `TokenManageable` adapter concern for standardized token read/write behavior across adapters.
- PKCE-based U2M OAuth support for the Databricks adapter.
- Expanded tests for OAuth and Databricks token flows.

### Changed

- Databricks adapter now requires explicit `auth_mode` to reduce ambiguous auth configuration.
- Updated documentation for adapter auth/token usage and adapter authoring.

## [0.3.0] - 2026-04-22

### Changed

- Added Databricks Adapter

## [0.2.1] - 2025-01-27

### Changed

- **Adapter missing-gem error messages** (Athena, DuckDB, MySQL, PostgreSQL, SQL Server, Trino): replace platform-specific system library install instructions with links to official documentation. Messages now include `gem install` and a single link for system libraries.

## [0.2.0] - 2025-10-12

### Added

- **SQLite adapter** with performance optimizations
  - WAL (Write-Ahead Logging) mode enabled by default for concurrent reads
  - Performance-tuned pragmas: cache_size, mmap_size, temp_store, synchronous
  - Custom date truncation for year, quarter, month, week, day, hour, minute, second
  - Custom day/month name extraction via CASE statements (SQLite lacks strftime %A/%B support)
  - Proper date casting using `date()` function
  - Comprehensive test suite and documentation
- **Redshift adapter** for AWS data warehouse
  - Native Redshift SQL function support
  - Full metadata and table introspection
- `date_time_literal` method for creating timestamp literals
- `date_lit` method for creating date literals

### Changed

- Removed ActiveSupport dependency
  - Replaced `symbolize_keys` with `transform_keys(&:to_sym)`
  - Replaced `demodulize` with `split('::').last.downcase`
  - Removed core extensions
- Standardized all SQL function names in settings to UPPERCASE for consistency

### Fixed

- Config defaults now properly set even when config key is passed with nil value
- Table instantiation issues resolved
- Test suite no longer requires Trino gem for default tests

## [0.1.0] - 2025-07-03

- Initial release
