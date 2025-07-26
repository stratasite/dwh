# frozen_string_literal: true

require "bundler/gem_tasks"
require "minitest/test_task"
require "standard/rake"

namespace :test do
  Minitest::TestTask.create :unit do |t|
    t.test_globs = ["test/unit/**/*_test.rb"]
  end
end

task test: %i[test:unit]
task default: %i[test standard]
