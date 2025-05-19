require_relative "../config/application"

namespace :benchmark do
  desc "Enqueue N jobs of a given type (e.g., fast, medium, slow)"
  task :enqueue, [:type, :count] => :environment do |_, args|
    job_class = "#{args[:type].capitalize}Job".constantize
    puts "Enqueuing #{args[:count]} #{args[:type]} jobs..."
    args[:count].to_i.times { job_class.perform_later }
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

    puts "\n\nRESULT:\n#{JSON.pretty_generate(result)}"

    Dir.mkdir("results") unless Dir.exist?("results")
    filename = "results/#{Time.now.strftime("%Y%m%d-%H%M")}-#{result[:adapter]}-#{type}.json"
    File.write(filename, JSON.dump(result))
    puts "Saved to #{filename}"
  end
end
