# frozen_string_literal: true

require_relative 'lib/dwh/version'

Gem::Specification.new do |spec|
  spec.name = 'dwh'
  spec.version = DWH::VERSION
  spec.authors = ['Ajo Abraham']
  spec.email = ['heyajo81@gmail.com']

  spec.summary = 'Adapters for interacting with popular data warehouses.'
  spec.description = 'Provides a unified interface across data warehouses to connect, execute, and introspect.'
  spec.homepage = 'https://www.strata.site'
  spec.required_ruby_version = '>= 3.4.4'

  spec.metadata['allowed_push_host'] = 'https://rubygems.org'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/stratasite/dwh.git'
  spec.metadata['changelog_uri'] = "TODO: Put your gem's CHANGELOG.md URL here."

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = 'exe'
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  # Uncomment to register a new dependency of your gem
  spec.add_dependency 'activesupport', '~> 8.0.2'
  spec.add_dependency 'connection_pool', '~> 2.4'
  spec.add_dependency 'csv', '~> 3.3.5'
  spec.add_dependency 'faraday'
  spec.add_dependency 'jwt', '~> 2.10.1'
  spec.add_dependency 'logger'
  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
