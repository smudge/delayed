# frozen_string_literal: true

# lib/benchmark_runner.rb

require "csv"
require "json"
require_relative "../lib/adapters/delayed"

module Benchmark
  class Runner
    def self.run_monitor(total_jobs:, job_type:, workers:)
      puts "Monitoring until #{total_jobs} jobs complete..."

      start_time = Time.now
      counts = []

      conn = ActiveRecord::Base.connection.raw_connection
      conn.exec('LISTEN delayed_jobs_count')

      remaining = total_jobs

      while remaining.positive?
        conn.wait_for_notify(10) do |_channel, _pid, payload|
          data = JSON.parse(payload)
          counts << data
          remaining = data['remaining'].to_i
          done = total_jobs.to_i - remaining
          elapsed = Time.parse(counts.last['at']) - start_time

          print "\r#{done}/#{total_jobs.to_i} done in #{elapsed.round(2)}s"
        end
      end

      end_time = Time.parse(counts.last['at'])
      duration = end_time - start_time
      result = {
        adapter: Adapters::Delayed.adapter_name,
        job_type: job_type,
        workers: workers,
        total_jobs: total_jobs,
        started_at: start_time.iso8601,
        finished_at: end_time.iso8601,
        duration_seconds: duration.round(2),
        jobs_per_second: (total_jobs / duration).round(2),
      }

      # Pickup query stats (pg_stat_statements)
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

      # Table-level query stats (pg_stat_user_tables)
      row = ActiveRecord::Base.connection.select_all(<<~SQL).first
        SELECT seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del
        FROM pg_stat_user_tables
        WHERE relname = 'delayed_jobs';
      SQL
      result.merge!(
        delayed_seq_scan: row["seq_scan"].to_i,
        delayed_idx_scan: row["idx_scan"].to_i,
        delayed_rows_inserted: row["n_tup_ins"].to_i,
        delayed_rows_deleted: row["n_tup_del"].to_i,
      )

      puts "\n\nRESULT:\n#{JSON.pretty_generate(result)}"

      FileUtils.mkdir_p("results")
      File.write("results/benchmark_result.json", JSON.pretty_generate(result))
    end

    def self.process_results
      result_file = "results/benchmark_result.json"
      explain_csvs = Dir["results/explain_samples-*.csv"].sort_by { |f| File.mtime(f) }

      unless File.exist?(result_file)
        puts "No benchmark result found. Run monitor first."
        return
      end

      result = JSON.parse(File.read(result_file))
      explain_samples = explain_csvs.last && CSV.read(explain_csvs.last, headers: true)

      puts "\n\n=== Benchmark Result ==="
      puts JSON.pretty_generate(result)

      if explain_samples && explain_samples.any?
        rows_removed = explain_samples.map { |row| row["rows_removed"].to_i }
        avg_removed = rows_removed.sum / rows_removed.size.to_f
        peak_removed = rows_removed.max

        puts "\n-- Pickup Contention --"
        puts "  Avg rows removed:  #{avg_removed.round(2)}"
        puts "  Peak rows removed: #{peak_removed}"
      end
    end
  end
end
