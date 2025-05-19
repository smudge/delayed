FROM ruby:3.2

ENV BUNDLE_PATH=/bundle \
    BUNDLE_APP_CONFIG=/bundle/config \
    BUNDLE_JOBS=4 \
    BUNDLE_RETRY=3

WORKDIR /app

RUN apt-get update -qq && apt-get install -y postgresql-client

COPY app/Gemfile app/Gemfile.lock ./
RUN bundle install

COPY app/ ./

CMD ["irb"]
