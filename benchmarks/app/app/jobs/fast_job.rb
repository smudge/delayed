# frozen_string_literal: true

class FastJob < ActiveJob::Base
  queue_as :default

  def perform
    sleep 0.005
  end
end
