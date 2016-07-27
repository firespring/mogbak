FROM ruby:2.2
MAINTAINER Firespring "info.dev@firespring.com"

RUN apt-get update \
    && apt-get install -y libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/

RUN mkdir -p /usr/src/app/lib
WORKDIR /usr/src/app

RUN gem install bundler
ENV GEM_HOME /usr/src/app/vendor/bundle/
ENV GEM_PATH /usr/src/app/vendor/bundle/:/usr/local/bundle/
ENV BUNDLE_PATH /usr/src/app/vendor/bundle/
ENV BUNDLE_BIN /usr/src/app/vendor/bundle/bin/
ENV PATH $PATH:/usr/src/app/vendor/bundle/bin/

# Configure nokogiri to use system libraries so we build faster
RUN bundle config build.nokogiri --use-system-libraries

# Copy library specs in which will invalidate the cache if any libraries have been changed
COPY Gemfile /usr/src/app/
COPY Gemfile.lock /usr/src/app/
COPY mogbak.gemspec /usr/src/app/
COPY lib/mogbak_version.rb /usr/src/app/lib/

# Copy in any existing libraries so we build faster
COPY vendor /usr/src/app/vendor

# Make sure we have all the bundles we need
RUN bundle install

# Copy in the actual api server code
COPY . /usr/src/app
VOLUME /usr/src/app/mirror.settings.yml

CMD ["/usr/local/bundle/bin/bundle","exec","bin/mogbak","--debug","mirror"]
