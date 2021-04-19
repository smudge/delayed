require 'helper'

describe Delayed::Worker do
  describe 'backend=' do
    before do
      @clazz = Class.new
      described_class.backend = @clazz
    end

    after do
      described_class.backend = :active_record
    end

    it 'sets the Delayed::Job constant to the backend' do
      expect(Delayed::Job).to eq(@clazz)
    end

    it 'sets backend with a symbol' do
      described_class.backend = :active_record
      expect(described_class.backend).to eq(Delayed::Backend::ActiveRecord::Job)
    end
  end

  describe 'job_say' do
    before do
      @worker = described_class.new
      @job = double('job', id: 123, name: 'ExampleJob', queue: nil)
    end

    it 'logs with job name and id' do
      expect(@job).to receive(:queue)
      expect(@worker).to receive(:say)
        .with('Job ExampleJob (id=123) message', described_class.default_log_level)
      @worker.job_say(@job, 'message')
    end

    it 'logs with job name, queue and id' do
      expect(@job).to receive(:queue).and_return('test')
      expect(@worker).to receive(:say)
        .with('Job ExampleJob (id=123) (queue=test) message', described_class.default_log_level)
      @worker.job_say(@job, 'message')
    end

    it 'has a configurable default log level' do
      described_class.default_log_level = 'error'

      expect(@worker).to receive(:say)
        .with('Job ExampleJob (id=123) message', 'error')
      @worker.job_say(@job, 'message')
    end
  end

  context 'worker read-ahead' do
    before do
      @read_ahead = described_class.read_ahead
    end

    after do
      described_class.read_ahead = @read_ahead
    end

    it 'reads five jobs' do
      expect(described_class.new.read_ahead).to eq(5)
    end

    it 'reads a configurable number of jobs' do
      described_class.read_ahead = 15
      expect(described_class.new.read_ahead).to eq(15)
    end
  end

  context 'worker exit on complete' do
    before do
      described_class.exit_on_complete = true
    end

    after do
      described_class.exit_on_complete = false
    end

    it 'exits the loop when no jobs are available' do
      worker = described_class.new
      Timeout.timeout(2) do
        worker.start
      end
    end
  end

  context 'worker job reservation' do
    before do
      described_class.exit_on_complete = true
    end

    after do
      described_class.exit_on_complete = false
    end

    it 'handles error during job reservation' do
      expect(Delayed::Job).to receive(:reserve).and_raise(Exception)
      described_class.new.work_off
    end

    it 'gives up after 10 backend failures' do
      expect(Delayed::Job).to receive(:reserve).exactly(10).times.and_raise(Exception)
      worker = described_class.new
      9.times { worker.work_off }
      expect(lambda { worker.work_off }).to raise_exception Delayed::FatalBackendError
    end

    it 'allows the backend to attempt recovery from reservation errors' do
      expect(Delayed::Job).to receive(:reserve).and_raise(Exception)
      expect(Delayed::Job).to receive(:recover_from).with(instance_of(Exception))
      described_class.new.work_off
    end
  end

  describe '#say' do
    before(:each) do
      @worker = described_class.new
      @worker.name = 'ExampleJob'
      @worker.logger = double('job')
      time = Time.now
      allow(Time).to receive(:now).and_return(time)
      @text = 'Job executed'
      @worker_name = '[Worker(ExampleJob)]'
      @expected_time = time.strftime('%FT%T%z')
    end

    after(:each) do
      @worker.logger = nil
    end

    shared_examples_for 'a worker which logs on the correct severity' do |severity|
      it "logs a message on the #{severity[:level].upcase} level given a string" do
        expect(@worker.logger).to receive(:send)
          .with(severity[:level], "#{@expected_time}: #{@worker_name} #{@text}")
        @worker.say(@text, severity[:level])
      end

      it "logs a message on the #{severity[:level].upcase} level given a fixnum" do
        expect(@worker.logger).to receive(:send)
          .with(severity[:level], "#{@expected_time}: #{@worker_name} #{@text}")
        @worker.say(@text, severity[:index])
      end
    end

    severities = [{ index: 0, level: 'debug' },
                  { index: 1, level: 'info' },
                  { index: 2, level: 'warn' },
                  { index: 3, level: 'error' },
                  { index: 4, level: 'fatal' },
                  { index: 5, level: 'unknown' }]
    severities.each do |severity|
      it_behaves_like 'a worker which logs on the correct severity', severity
    end

    it 'logs a message on the default log\'s level' do
      expect(@worker.logger).to receive(:send)
        .with('info', "#{@expected_time}: #{@worker_name} #{@text}")
      @worker.say(@text, described_class.default_log_level)
    end
  end

  describe 'plugin registration' do
    it 'does not double-register plugins on worker instantiation' do
      performances = 0
      plugin = Class.new(Delayed::Plugin) do
        callbacks do |lifecycle|
          lifecycle.before(:enqueue) { performances += 1 }
        end
      end
      described_class.plugins << plugin

      described_class.new
      described_class.new
      described_class.lifecycle.run_callbacks(:enqueue, nil) {} # no-op

      expect(performances).to eq(1)
    end
  end

  describe 'thread callback' do
    it 'wraps code after thread is checked out' do
      performances = Concurrent::AtomicFixnum.new(0)
      plugin = Class.new(Delayed::Plugin) do
        callbacks do |lifecycle|
          lifecycle.before(:thread) { performances.increment }
        end
      end
      described_class.plugins << plugin

      Delayed::Job.delete_all
      Delayed::Job.enqueue SimpleJob.new
      worker = described_class.new

      worker.work_off

      expect(performances.value).to eq(1)
    end
  end
end
