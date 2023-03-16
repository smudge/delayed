require 'helper'

RSpec.describe Delayed::Plugins::Instrumentation do
  let!(:job) { Delayed::Job.enqueue JobWithArgs.new('arg', kwarg: 'kwarg'), priority: 13, queue: 'test' }

  it 'emits delayed.job.run' do
    expect { Delayed::Worker.new.work_off }.to emit_notification('delayed.job.run').with_payload(
      job_name: 'JobWithArgs',
      priority: 13,
      queue: 'test',
      table: 'delayed_jobs',
      database: current_database,
      database_adapter: current_adapter,
      job: job,
    )
  end

  context 'with a second (activejob) job' do
    let(:aj) { ActiveJobJobWithArgs.new('arg', kwarg: 'kwarg').serialize }
    let(:aj_wrapper) { ActiveJob::QueueAdapters::DelayedJobAdapter::JobWrapper.new(aj) }
    let!(:active_job_job) { Delayed::Job.enqueue aj_wrapper }

    it 'emits delayed.job.run twice' do
      expect { Delayed::Worker.new.work_off }.to emit_notification('delayed.job.run').with_payload(
        job_name: 'JobWithArgs',
        priority: 13,
        queue: 'test',
        table: 'delayed_jobs',
        database: current_database,
        database_adapter: current_adapter,
        job: job,
      ).and emit_notification('delayed.job.run').with_payload(
        job_name: 'ActiveJobJobWithArgs',
        priority: 10,
        queue: 'default',
        table: 'delayed_jobs',
        database: current_database,
        database_adapter: current_adapter,
        job: active_job_job,
      )
    end
  end

  context 'when the job errors' do
    let!(:job) { Delayed::Job.enqueue ErrorJob.new, priority: 7, queue: 'foo' }

    it 'emits delayed.job.error' do
      expect { Delayed::Worker.new.work_off }.to emit_notification('delayed.job.error').with_payload(
        job_name: 'ErrorJob',
        priority: 7,
        queue: 'foo',
        table: 'delayed_jobs',
        database: current_database,
        database_adapter: current_adapter,
        job: job,
      )
    end
  end

  context 'when the job fails' do
    let!(:job) { Delayed::Job.enqueue FailureJob.new, priority: 3, queue: 'bar' }

    it 'emits delayed.job.failure' do
      expect { Delayed::Worker.new.work_off }.to emit_notification('delayed.job.failure').with_payload(
        job_name: 'FailureJob',
        priority: 3,
        queue: 'bar',
        table: 'delayed_jobs',
        database: current_database,
        database_adapter: current_adapter,
        job: job,
      )
    end
  end
end
