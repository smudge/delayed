# frozen_string_literal: true

require "json"
require "active_support/notifications"
require "csv"

module Adapters
  class Delayed
    class << self
      def adapter_name
        "delayed"
      end

      def start_sampler!
        @last_sample_time = 0

        filename = "results/explain_samples.csv"
        FileUtils.mkdir_p("results")
        FileUtils.rm_f(filename)

        Thread.new do
          ActiveSupport::Notifications.subscribe("sql.active_record") do |_, _, _, _, payload|
            sql = payload[:sql]
            now = Time.now.to_i

            next unless now - @last_sample_time > 2
            next unless sql.include?('UPDATE "delayed_jobs"') && sql.include?("RETURNING") && !sql.include?("EXPLAIN")

            begin
              explain_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) #{sql}"
              plan_json = ActiveRecord::Base.connection.exec_query(explain_sql).first["QUERY PLAN"]
              plan = JSON.parse(plan_json).first

              extract_summary(plan).tap do |summary|
                unless File.exist?(filename)
                  File.open(filename, "a") do |f|
                    f.flock(File::LOCK_EX)
                    f.puts summary.keys.to_csv
                  end
                end

                File.open(filename, "a") do |f|
                  f.flock(File::LOCK_EX)
                  f.puts summary.values.to_csv
                end
              end

              @last_sample_time = now
            rescue StandardError => e
              warn "EXPLAIN failed: #{e.message}"
            end
          end
        end
      end

      private

      def extract_summary(plan)
        inner_nodes = leaf_nodes(plan.fetch("Plan"))
        all_nodes = flatten_plan(plan.fetch("Plan"))

        inner_metrics = aggregate_metrics(inner_nodes)
        overall_metrics = aggregate_metrics(all_nodes)

        {
          timestamp: Time.now.iso8601,
          node_types: all_nodes.map { _1.fetch("Node Type") }.uniq.sort.join("|"),
          sort_methods: all_nodes.map { _1["Sort Method"] }.compact.uniq.sort.join("|"),
          index_names: all_nodes.map { _1["Index Name"] }.compact.uniq.sort.join("|"),
          inner_rows: inner_metrics[:rows],
          inner_loops: inner_metrics[:loops],
          inner_rows_skipped: inner_metrics[:rows_skipped],
          inner_time: inner_metrics[:time],
          inner_buffers_hit: inner_metrics[:buffers_hit],
          inner_buffers_read: inner_metrics[:buffers_read],
          inner_buffers_written: inner_metrics[:buffers_written],
          overall_rows: overall_metrics[:rows],
          overall_loops: overall_metrics[:loops],
          overall_rows_skipped: overall_metrics[:rows_skipped],
          overall_time: overall_metrics[:time],
          overall_buffers_hit: overall_metrics[:buffers_hit],
          overall_buffers_read: overall_metrics[:buffers_read],
          overall_buffers_written: overall_metrics[:buffers_written],
        }
      end

      def flatten_plan(node)
        [node] + (node["Plans"]&.flat_map { |child| flatten_plan(child) } || [])
      end

      def leaf_nodes(node)
        return [node] unless node["Plans"]&.any?

        node["Plans"].flat_map { |child| leaf_nodes(child) }
      end

      def aggregate_metrics(nodes)
        {
          rows: nodes.sum { _1.fetch("Actual Rows") * _1.fetch("Actual Loops") },
          loops: nodes.sum { _1.fetch("Actual Loops") },
          rows_skipped: nodes.sum { _1["Rows Removed by Filter"] || 0 },
          time: nodes.sum { _1.fetch("Actual Total Time") },
          buffers_hit: nodes.sum { _1.fetch("Shared Hit Blocks") },
          buffers_read: nodes.sum { _1.fetch("Shared Read Blocks") },
          buffers_written: nodes.sum { _1.fetch("Shared Written Blocks") },
        }
      end
    end
  end
end
