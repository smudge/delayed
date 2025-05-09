module Delayed
  module Runnable
    def start
      trap('TERM') { quit! }
      trap('INT') { quit! }

      say "Starting #{self.class.name}"

      Delayed.lifecycle.run_callbacks(:execute, nil) do
        loop do
          run!
          break if stop?
        end
      end
    ensure
      on_exit!
    end

    private

    def on_exit!; end

    def interruptable_sleep(seconds)
      pipe[0].wait_readable(seconds) if seconds.positive?
    end

    def stop
      pipe[1].close
    end

    def stop?
      pipe[1].closed?
    end

    def quit!
      Thread.new { say 'Exiting...' }.tap do |t|
        stop
        t.join
      end
    end

    def pipe
      @pipe ||= IO.pipe
    end

    def duty_cycle(max_duty_cycle, at_least: 0)
      work_start = clock_time

      if instance_variable_defined?(:@dc_work_duration)
        min_work_interval = @dc_work_duration * (1 - max_duty_cycle)
        time_since_last_work = work_start - @dc_work_end
        interruptable_sleep([min_work_interval - time_since_last_work, at_least].max)
      end

      yield.tap do
        @dc_work_end = clock_time
        @dc_work_duration = @dc_work_end - work_start
      end
    end

    def clock_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
