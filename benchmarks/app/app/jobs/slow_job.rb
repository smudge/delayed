# frozen_string_literal: true

class SlowJob < ActiveJob::Base
  queue_as :default

  def perform
    sleep 2
  end
end
