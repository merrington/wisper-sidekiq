source 'https://rubygems.org'

gemspec

gem 'bundler'
gem 'rake'
gem 'rspec'
gem 'coveralls', require: false

gem 'redis', '<= 4.0.3' if RUBY_VERSION < '2.3'

gem 'psych', platforms: :rbx

gem 'wisper', git: 'https://github.com/merrington/wisper.git', branch: 'ruby-3'

group :extras do
  gem 'rerun'
  gem 'pry-byebug'
end
