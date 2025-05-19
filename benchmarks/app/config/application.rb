# frozen_string_literal: true

require "active_job"
require "active_job/railtie"
require "active_record"
require "active_record/railtie"
require "delayed"
require "logger"

ActiveRecord::Base.establish_connection(ENV["DATABASE_URL"])
ActiveRecord::Base.logger = Logger.new(STDOUT)

class BenchmarkApp < Rails::Application
  config.load_defaults 7.1
  config.eager_load = false
  config.active_job.queue_adapter = :delayed
end
