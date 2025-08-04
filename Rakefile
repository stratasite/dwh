# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'minitest/test_task'

namespace :test do
  Minitest::TestTask.create :unit do |t|
    t.test_globs = ['test/unit/**/*_test.rb']
  end

  namespace :system do
    task :setup, [:db_type, :setup_file] do |_, args|
      db_type = args[:db_type]
      file = "test/support/compose.#{db_type}.yml"
      system("docker compose -f #{file} up -d") or raise "Failed to start #{db_type} containers"

      puts 'waiting 10 secs for docker....'
      sleep(10)
      loop do
        res = system("docker compose -f #{file} ps --services --filter status=running")

        if res
          puts "Checking for setup file #{args[:setup_file]}"
          if args[:setup_file]
            require_relative args[:setup_file]
            sleep(10)
            DWH::TestSetup.run
          end
          break
        end

        sleep(1)
      end
    end

    task :teardown, [:db_type] do |_, args|
      db_type = args[:db_type]
      system("docker compose -f test/support/compose.#{db_type}.yml down") or raise "Failed to stop #{db_type} containers"
    end

    task :run, [:db_type, :setup_file] do |_, args|
      db_type = args[:db_type]
      Rake::Task['test:system:setup'].invoke(db_type, args[:setup_file])
      Dir["test/system/**/#{db_type}_*.rb"].each do |f|
        system("ruby -Ilib:test #{f}")
      end
      puts "Completed #{db_type} system tests"
    ensure
      Rake::Task['test:system:teardown'].invoke(db_type)
    end

    desc 'Run RDBMS tests. This will start docker compose as needed'
    task :rdbms do
      Rake::Task['test:system:run'].invoke('rdbms')
    end

    desc 'Run Druid tests. This will start docker compose as needed'
    task :druid do
      Rake::Task['test:system:run'].invoke('druid', 'test/support/druid/test_setup.rb')
    end
  end
end

# task test: %i[test:unit]
# task test_all: %i[test:unit test:system:all]
