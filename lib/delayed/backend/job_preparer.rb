module Delayed
  module Backend
    class JobPreparer
      attr_reader :options, :args

      def initialize(*args)
        @options = args.extract_options!.dup
        @args = args
      end

      def prepare
        set_payload
        if defined?(ActiveJob)
          detect_unwrapped_active_job!
          handle_legacy_active_job_adapter!
        end
        set_queue_name
        set_priority
        handle_deprecation
        options
      end

      private

      def set_payload
        options[:payload_object] ||= args.shift
      end

      def detect_unwrapped_active_job!
        raise(ConfigurationError, <<~MSG) if options[:payload_object].is_a?(ActiveJob::Base)
          ActiveJob classes cannot be enqueued directly!

          Please call #{options[:payload_object].class}.perform_later(...), or read more here:
          https://guides.rubyonrails.org/active_job_basics.html
        MSG
      end

      def handle_legacy_active_job_adapter!
        raise(ConfigurationError, <<~MSG) if options[:payload_object].is_a?(ActiveJob::QueueAdapters::DelayedJobAdapter::JobWrapper)
          Incompatible ActiveJob configuration detected!

          Please configure ActiveJob to use :delayed adapter.
        MSG
      end

      def set_queue_name
        options[:queue] ||= options[:payload_object].queue_name if options[:payload_object].respond_to?(:queue_name)
        options[:queue] ||= Delayed::Worker.default_queue_name
      end

      def set_priority
        options[:priority] ||= options[:payload_object].priority if options[:payload_object].respond_to?(:priority)
        options[:priority] ||= Delayed::Worker.default_priority
      end

      def handle_deprecation
        unless options[:payload_object].respond_to?(:perform)
          raise ArgumentError,
                'Cannot enqueue items which do not respond to perform'
        end
      end
    end
  end
end
