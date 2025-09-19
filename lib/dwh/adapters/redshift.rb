module DWH
  module Adapters
    # Redshift adapter. Please ensure the pg gem is available before using this adapter.
    # Generally, adapters should be created using {DWH::Factory#create DWH.create}. Where a configuration
    # is passed in as options hash or argument list.
    #
    # @example Basic connection with required only options
    #   DWH.create(:redshift, {host: 'localhost', database: 'redshift',
    #     username: 'redshift'})
    #
    # @example Connection with cert based SSL connection
    #   DWH.create(:redshift, {host: 'localhost', database: 'redshift',
    #     username: 'redshift', ssl: true,
    #     extra_connection_params: { sslmode: 'require' })
    #
    #   valid sslmodes: disable, prefer, require, verify-ca, verify-full
    #   For modes requiring Certs make sure you add the appropirate params
    #   to extra_connection_params. (ie sslrootcert, sslcert etc.)
    #
    # @example Connection sending custom application name
    #   DWH.create(:redshift, {host: 'localhost', database: 'redshift',
    #     username: 'redshift', application_name: "Strata CLI" })
    class Redshift < Postgres
      config :host, String, required: true, message: 'server host ip address or domain name'
      config :port, Integer, required: false, default: 5439, message: 'port to connect to'
      config :database, String, required: true, message: 'name of database to connect to'
      config :schema, String, default: 'public', message: 'schema name. defaults to "public"'
      config :username, String, required: true, message: 'connection username'
      config :password, String, required: false, default: nil, message: 'connection password'
      config :query_timeout, Integer, required: false, default: 3600, message: 'query execution timeout in seconds'
      config :client_name, String, required: false, default: 'DWH Ruby Gem', message: 'The name of the connecting app'
      config :ssl, Boolean, required: false, default: false, message: 'use ssl'

      # Need to override default add method
      # since redshift doesn't support quarter as an
      # interval.
      # @param unit [String] Should be one of day, month, quarter etc
      # @param val [String, Integer] The number of days to add
      # @param exp [String] The sql expresssion to modify
      def date_add(unit, val, exp)
        gsk(:date_add)
          .gsub(/@unit/i, unit)
          .gsub(/@val/i, val.to_s)
          .gsub(/@exp/i, exp)
      end
    end
  end
end
