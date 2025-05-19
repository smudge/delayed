# frozen_string_literal: true

class MediumJob < ActiveJob::Base
  queue_as :default

  def perform
    sleep 0.25
  end
end
