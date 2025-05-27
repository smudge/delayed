# frozen_string_literal: true

require "json"
require "active_support/notifications"
require "concurrent"

module Adapters
  class Delayed
    @explain_results = Concurrent::Array.new
    @last_sample_time = Concurrent::AtomicFixnum.new(0)
    @sampler_started = Concurrent::AtomicBoolean.new(false)

    class << self
      def adapter_name
        "delayed"
      end

      def start_sampler!
        return if @sampler_started.true?

        @sampler_thread ||= Thread.new do
          ActiveSupport::Notifications.subscribe("sql.active_record") do |_, _, _, _, payload|
            sql = payload[:sql]
            now = Time.now.to_i
            next unless now - @last_sample_time.value > 2
            next unless sql.include?('UPDATE "delayed_jobs"') && sql.include?("RETURNING") && sql.exclude?("EXPLAIN")

            begin
              explain_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) #{sql}"
              plan_json = ActiveRecord::Base.connection.exec_query(explain_sql).first["QUERY PLAN"]
              plan = JSON.parse(plan_json).first

              @explain_results << extract_summary(plan)
              @last_sample_time.value = now
            rescue StandardError => e
              puts "EXPLAIN failed: #{e.message}"
            end
          end
        end

        @sampler_started.make_true
      end

      def explain_samples
        @explain_results.dup
      end

      def extract_summary(plan)
        scan = plan.dig("Plans", 0, "Plans", 0) || {}
        {
          timestamp: Time.now.iso8601,
          rows_removed: scan["Rows Removed by Filter"] || 0,
          actual_rows: scan["Actual Rows"] || 0,
          buffers_read: scan.dig("Buffers", "Shared Read") || 0,
          buffers_hit: scan.dig("Buffers", "Shared Hit") || 0,
          total_time: plan["Actual Total Time"],
        }
      end
    end
  end
end
