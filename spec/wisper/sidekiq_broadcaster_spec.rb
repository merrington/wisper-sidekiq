require 'wisper/sidekiq'

RSpec.describe Wisper::SidekiqBroadcaster do
  class PublisherUnderTest
    include Wisper::Publisher

    def run
      broadcast(:it_happened)
    end
  end

  class RegularSubscriberUnderTest
    def self.it_happened(*_args, **_kwargs)
      binding.pry
    end
  end

  class CustomizedSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_options
      { queue: "my_queue" }
    end
  end

  class CustomizedScheduleInJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { perform_in: 5 }
    end
  end

  class CustomizedEventScheduleInJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { it_happened: { perform_in: 5 } }
    end
  end

  class CustomizedScheduleAtJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { perform_at: Time.now + 5 }
    end
  end

  class CustomizedEventScheduleAtJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { it_happened: { perform_at: Time.now + 5 } }
    end
  end

  class CustomizedBadScheduleInJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { perform_in: 'not a number', delay: 5 }
    end
  end

  class CustomizedBadDefaultScheduleInWithEventScheduleAtJobSubscriberUnderTest
    def self.it_happened
    end

    def self.sidekiq_schedule_options
      { perform_in: 'not a number', delay: 5, it_happened: { perform_at: Time.now + 5 } }
    end
  end

  let(:publisher) { PublisherUnderTest.new }

  before { Sidekiq::Testing.fake! }
  after { Sidekiq::Testing.disable! }

  describe '#broadcast' do
    it 'schedules a sidekiq job' do
      publisher.subscribe(RegularSubscriberUnderTest, async: described_class.new)

      expect { publisher.run }
        .to change(Sidekiq::Queues["default"], :size).by(1)
    end

    it 'schedules to run in some time a sidekiq job' do
      publisher.subscribe(CustomizedScheduleInJobSubscriberUnderTest, async: described_class.new)

      # In order to look into Sidekiq::ScheduledSet we need to hit redis
      expect { publisher.run }
        .to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }.by(1)
    end

    it 'schedules to run in some time a sidekiq job for an event' do
      publisher.subscribe(CustomizedEventScheduleInJobSubscriberUnderTest, async: described_class.new)

      # In order to look into Sidekiq::ScheduledSet we need to hit redis
      expect { publisher.run }
        .to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }.by(1)
    end

    it 'schedules to run at some time a sidekiq job' do
      publisher.subscribe(CustomizedEventScheduleAtJobSubscriberUnderTest, async: described_class.new)

      # In order to look into Sidekiq::ScheduledSet we need to hit redis
      expect { publisher.run }
        .to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }.by(1)
    end

    it 'schedules to run at some time a sidekiq job for an event' do
      publisher.subscribe(CustomizedEventScheduleInJobSubscriberUnderTest, async: described_class.new)

      # In order to look into Sidekiq::ScheduledSet we need to hit redis
      expect { publisher.run }
        .to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }.by(1)
    end

    it 'can respect custom sidekiq_options' do
      publisher.subscribe(CustomizedSubscriberUnderTest, async: described_class.new)

      expect { publisher.run }
        .to change(Sidekiq::Queues["my_queue"], :size).by(1)
    end

    it 'schedules a sidekiq job with bad sidekiq_schedule_options' do
      publisher.subscribe(CustomizedBadScheduleInJobSubscriberUnderTest, async: described_class.new)

      expect { publisher.run }
        .to change(Sidekiq::Queues["default"], :size).by(1)
      expect { publisher.run }
        .not_to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }
    end

    it 'schedules a sidekiq job with bad sidekiq_schedule_options' do
      publisher.subscribe(CustomizedBadDefaultScheduleInWithEventScheduleAtJobSubscriberUnderTest, async: described_class.new)

      expect { publisher.run }
        .to change { Sidekiq::Queues["default"].select{|job| job.key?('at')}.size }.by(1)
    end

    context 'when provides subscriber with args' do
      let(:subscriber) { RegularSubscriberUnderTest }
      let(:event) { 'it_happened' }

      context 'with positional arguments only' do
        let(:args) { [1,2,3] }

        subject(:broadcast_event) { described_class.new.broadcast(subscriber, nil, event, *args) }

        fit 'subscriber receives event with corrects args' do
          expect(RegularSubscriberUnderTest).to receive(event).with(*args, **{})
          # **{} is needed because of how it evaluates under different version of ruby
          # `#broadcast` will be receiving `**kwargs = nil` and write this as {} to sidekiq/redis. When the 
          # SidekiqBroadcaster::Worker class parses the yaml and calls the method, it will be passing
          # `**{}` to as the kwargs - in ruby 2.7 this is treated as an empty hash, but in ruby 3.0 it isn't passed at all

          Sidekiq::Testing.inline! { broadcast_event }
        end
      end

      context 'with keyword arguments only' do
        let(:kwargs) { { a: 1, b: 2, c: 3 } }

        subject(:broadcast_event) { described_class.new.broadcast(subscriber, nil, event, **kwargs) }

        it 'subscriber receives event with corrects args' do
          expect(RegularSubscriberUnderTest).to receive(event).with(**kwargs)

          Sidekiq::Testing.inline! { broadcast_event }
        end
      end

      context 'with positional and keyword arguments' do
        let(:args) { [1,2,3] }
        let(:kwargs) { { a: 1, b: 2, c: 3 } }

        subject(:broadcast_event) { described_class.new.broadcast(subscriber, nil, event, *args, **kwargs) }

        it 'subscriber receives event with corrects args' do
          expect(RegularSubscriberUnderTest).to receive(event).with(*args, **kwargs)

          Sidekiq::Testing.inline! { broadcast_event }
        end
      end

      context 'message published under previous version' do
        let(:kwargs) { { a: 1, b: 2, c: 3 } }

        subject(:broadcast_event) do
          # This is what the previous version of Wisper::SidekiqBroadcaster did when publishing
          Wisper::SidekiqBroadcaster::Worker.perform_in(0, ::YAML.dump([subscriber, event, kwargs]))
        end

        it 'subscriber receives event with corrects args' do
          expect(RegularSubscriberUnderTest).to receive(event).with(**kwargs)

          Sidekiq::Testing.inline! { broadcast_event }
        end
      end
    end
  end
end
