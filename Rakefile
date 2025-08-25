# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'minitest/test_task'
require 'yard'

YARD::Rake::YardocTask.new(:doc) do |t|
  t.files = ['lib/**/*.rb']

  # Output directory
  t.options = [
    '--output-dir', 'docs',
    '--markup', 'markdown',
    '--markup-provider', 'redcarpet',
    '--charset', 'utf-8',
    '--verbose', '--debug',
    '--files', 'docs/guides/*.md'
  ]
end

namespace :test do
  Minitest::TestTask.create :unit do |t|
    t.test_globs = ['test/unit/**/*_test.rb']
  end

  namespace :system do
    Minitest::TestTask.create :rdbms do |t|
      t.test_globs = ['test/system/rdbms_*_test.rb']
    end

    Minitest::TestTask.create :druid do |t|
      t.test_globs = ['test/system/druid_test.rb']
    end

    Minitest::TestTask.create :cloud do |t|
      t.test_globs = ['test/system/cloud_test.rb']
    end
  end
end

# task test: %i[test:unit]
# task test_all: %i[test:unit test:system:all]
