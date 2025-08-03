require 'net/http'
require 'json'

module DWH
  module TestSetup
    INIT_SCRIPT = '01_init.sql'.freeze

    SPEC = {
      'type' => 'index_parallel',
      'spec' => {
        'dataSchema' => {
          'dataSource' => '',
          'timestampSpec' => {
            'column' => 'created_at',
            'format' => 'auto'
          },
          'dimensionsSpec' => {
            'useSchemaDiscovery' => true,
            'dimensionExclusions' => []
          },
          'granularitySpec' => {
            'segmentGranularity' => 'day',
            'queryGranularity' => 'none',
            'rollup' => false
          }
        },
        'ioConfig' => {
          'type' => 'index_parallel',
          'inputSource' => {
            'type' => 'local',
            'baseDir' => '/tmp/druid-data',
            'filter' => ''
          },
          'inputFormat' => {
            'type' => 'csv',
            'findColumnsFromHeader' => true
          }
        },
        'tuningConfig' => {
          'type' => 'index_parallel',
          'partitionsSpec' => { 'type' => 'dynamic' }
        }
      }
    }.freeze

    module_function

    def already_initialized?
      uri = URI('http://localhost:8081/druid/coordinator/v1/datasources')
      http = Net::HTTP.new(uri.host, uri.port)
      request = Net::HTTP::Get.new(uri)
      request['Content-Type'] = 'application/json'
      response = http.request(request)
      res = JSON.parse(response.body)
      !res.empty?
    end

    def init_statements
      puts __dir__
      script = File.read(File.join(__dir__, INIT_SCRIPT))
      script.split(';').map(&:strip).reject(&:empty?)
    end

    def run
      puts 'setting up druid'
      if already_initialized?
        puts 'already initialized'
        return
      end

      submit_tasks

      until pending_tasks.empty?
        puts 'waiting on pending tasks'
        sleep(1)
      end
    end

    def submit_tasks
      uri = URI('http://localhost:8081/druid/indexer/v1/task')
      %w[users posts].each do |ds|
        spec = SPEC.dup
        spec['spec']['dataSchema']['dataSource'] = ds
        spec['spec']['ioConfig']['inputSource']['filter'] = "#{ds}.csv"
        http = Net::HTTP.new(uri.host, uri.port)
        request = Net::HTTP::Post.new(uri)
        request['Content-Type'] = 'application/json'
        request.body = spec.to_json
        response = http.request(request)
        unless response.code == '200'
          puts "⚠️  Warning: Druid statement failed (#{response.code}): #{ds}..."
          puts "Response: #{response.body[0..200]}..."
        end
      end
    rescue StandardError => e
      puts 'Failed to initialize druid'
      raise e
    end

    def pending_tasks
      uri = URI('http://localhost:8081/druid/indexer/v1/runningTasks')
      http = Net::HTTP.new(uri.host, uri.port)
      request = Net::HTTP::Get.new(uri)
      request['Content-Type'] = 'application/json'
      response = http.request(request)
      JSON.parse(response.body)
    end
  end
end
