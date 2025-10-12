## [Unreleased]

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
