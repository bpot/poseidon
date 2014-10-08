require 'integration/simple/spec_helper'

RSpec.describe "truncated messages", :type => :request do
  before(:each) do
    @s1 = "a" * 335
    @s2 = "b" * 338

    @producer = Producer.new(["localhost:9092"],
                             "test_client",
                             :type => :sync)

    @producer.send_messages([Message.new(:topic => 'test_max_bytes', :value => @s1), Message.new(:topic => 'test_max_bytes', :value => @s2)])
  end

  it "correctly handles max_byte lengths smallert than a message" do
    0.upto(360) do |n|
      consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                       "test_max_bytes", 0, :earliest_offset)
      expect(consumer.fetch(:max_bytes => n)).to eq([])
    end
  end

  it "correctly handles max_byte lengths that should return a single message" do
    361.upto(724) do |n|
      consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                       "test_max_bytes", 0, :earliest_offset)

      messages = consumer.fetch(:max_bytes => n)
      expect(messages.size).to eq(1)
      expect(messages.first.value).to eq(@s1)
    end
  end

  it "correctly handles max_byte lengths that should return two messages" do
    725.upto(1000) do |n|
      consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                       "test_max_bytes", 0, :earliest_offset)

      messages = consumer.fetch(:max_bytes => n)
      expect(messages.size).to eq(2)
      expect(messages.map(&:value)).to eq([@s1, @s2])
    end
  end
end
