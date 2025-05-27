# frozen_string_literal: true

require "active_job"
require "active_job/railtie"
require "active_record"
require "active_record/railtie"
require "delayed"

ActiveRecord::Base.establish_connection(ENV.fetch('DATABASE_URL'))

class BenchmarkApp < Rails::Application
  config.load_defaults 7.1
  config.eager_load = false
  config.active_job.queue_adapter = :delayed
end
