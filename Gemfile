# frozen_string_literal: true

source 'https://rubygems.org'

# Specify your gem's dependencies in dwh.gemspec
gemspec

group :development do
  gem 'irb'
  # Clients used by DWH to connect to databases.
  # Users will have to add these to their own Gemfile
  # as needed.
  gem 'duckdb'
  gem 'mysql2'
  gem 'pg'
  gem 'tiny_tds'
  gem 'trino-client'

  # aws Athena
  gem 'aws-sdk-athena'
  gem 'aws-sdk-s3'
end

group :test do
  gem 'minitest', '~> 5.16'
end

group :development, :test do
  gem 'debug'

  # for documentation
  gem 'kramdown'
  gem 'yard'

  gem 'rake', '~> 13.0'
end
