require_relative "../config/application"

namespace :benchmark do
  desc "Enqueue N jobs of a given type (e.g., fast, medium, slow)"
  task :enqueue, [:type, :count] => :environment do |_, args|
    job_class = "#{args[:type].capitalize}Job".constantize
    puts "Enqueuing #{args[:count]} #{args[:type]} jobs..."
    args[:count].to_i.times { job_class.perform_later }

    ActiveRecord::Base.connection.execute('SELECT pg_stat_statements_reset();')
  end

  desc "Run worker loop (for manual scaling)"
  task :run_worker => :environment do
    puts "Starting worker loop..."
    Delayed::Worker.new.start
  end

  desc "Monitor and log benchmark results until queue is drained"
  task :run, [:type, :workers] => :environment do |_, args|
    type = args[:type] || "fast"
    workers = (args[:workers] || "1").to_i

    start = Time.now
    initial = Delayed::Job.count

    puts "Monitoring #{initial} #{type} jobs with #{workers} worker(s)..."

    loop do
      remaining = Delayed::Job.count
      done = initial - remaining
      elapsed = Time.now - start

      print "\r#{done}/#{initial} done in #{elapsed.round(2)}s"
      break if remaining == 0
      sleep 1
    end

    total = Time.now - start

    result = {
      adapter: "delayed",
      job_type: type,
      workers: workers,
      total_jobs: initial,
      duration_seconds: total.round(2),
      jobs_per_second: (initial / total).round(2)
    }

    puts "\n\n-- Worker contention stats (pg_stat_statements) --"
    row = ActiveRecord::Base.connection.select_all(<<~SQL).first
      SELECT
        calls,
        total_exec_time::numeric(10,2),
        mean_exec_time::numeric(10,2),
        rows,
        (rows / NULLIF(calls, 0))::numeric(10,2) AS avg_rows_per_call
      FROM pg_stat_statements
      WHERE query LIKE '%FROM "delayed_jobs"%'
        AND query LIKE '%FOR UPDATE%'
      ORDER BY total_exec_time DESC
      LIMIT 1;
    SQL

    result.merge!(
      pickup_query_calls: row["calls"].to_i,
      pickup_total_time_ms: row["total_time"].to_f,
      pickup_mean_time_ms: row["mean_time"].to_f,
      pickup_rows: row["rows"].to_i,
      pickup_avg_rows_per_call: row["avg_rows_per_call"].to_f,
    )

    puts "\n\nRESULT:\n#{JSON.pretty_generate(result)}"

    Dir.mkdir("results") unless Dir.exist?("results")
    filename = "results/#{Time.now.strftime("%Y%m%d-%H%M")}-#{result[:adapter]}-#{type}.json"
    File.write(filename, JSON.dump(result))
    puts "Saved to #{filename}"
  end
end
