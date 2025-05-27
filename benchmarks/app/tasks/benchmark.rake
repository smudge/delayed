# frozen_string_literal: true

require_relative "../config/application"
require_relative "../lib/adapters/delayed"
require_relative "../lib/benchmark_runner"

namespace :benchmark do
  desc "Enqueue N jobs of a given type (e.g., fast, medium, slow)"
  task :enqueue, %i(type count) => :environment do |_, args|
    job_class = "#{args[:type].capitalize}Job".constantize
    puts "Enqueuing #{args[:count]} #{args[:type]} jobs..."
    args[:count].to_i.times { job_class.perform_later }

    puts "Clearing pg_stat_* tables..."
    ActiveRecord::Base.connection.execute('SELECT pg_stat_statements_reset();')
    ActiveRecord::Base.connection.execute('SELECT pg_stat_reset();')
  end

  desc "Run worker loop (for manual scaling)"
  task run_worker: :environment do
    if ENV["EXPLAIN_SAMPLER"] == "1"
      puts "Starting EXPLAIN sampler..."
      Adapters::Delayed.start_sampler!
    end

    puts "Starting worker loop..."
    Delayed::Worker.new.start
  end

  desc "Monitor job queue until drained, and write benchmark result"
  task :monitor, %i(type count workers) => :environment do |_, args|
    type = args[:type] || "fast"
    count = args[:count].to_i
    workers = args[:workers].to_i

    Benchmark::Runner.run_monitor(
      total_jobs: count,
      job_type: type,
      workers: workers,
    )
  end

  desc "Process and report benchmark results"
  task process: :environment do
    Benchmark::Runner.process_results
  end
end
