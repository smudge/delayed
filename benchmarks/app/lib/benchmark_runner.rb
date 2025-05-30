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

      # EXPLAIN samples (reported by first worker)
      explain_samples = CSV.read("results/explain_samples.csv", headers: true)
      string_keys = %w(timestamp node_types sort_methods index_names).freeze
      result["explain_samples"] = explain_samples.size
      (explain_samples.first.headers - string_keys).each do |key|
        values = explain_samples.map { |r| r[key].to_f }
        result.merge!(
          "explain_#{key}" => {
            "avg" => values.sum / values.size.to_f,
            "max" => values.max,
            "min" => values.min,
            "median" => values.sort[(values.size * 0.5).ceil - 1],
            "90p" => values.sort[(values.size * 0.90).ceil - 1],
            "99p" => values.sort[(values.size * 0.99).ceil - 1],
          },
        )
      end

      puts "\n\nRESULT:\n#{JSON.pretty_generate(result)}"

      FileUtils.mkdir_p("results")
      File.write("results/benchmark_result.json", JSON.pretty_generate(result))
    end
  end
end
