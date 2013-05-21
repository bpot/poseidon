require 'integration/simple/spec_helper'

describe "simple producer and consumer" do

  describe "writing and consuming one topic" do
    it "fetches produced messages" do
      @producer = Producer.new(["localhost:9092"],
                               "test_client",
                               :type => :sync)


      messages = [MessageToSend.new("topic_simple_producer_and_consumer", "Hello World")]
      expect(@producer.send_messages(messages)).to eq(true)

      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -2)
      messages = @consumer.fetch
      expect(messages.last.value).to eq("Hello World")

      @producer.shutdown
    end

    it "fetches only messages since the last offset" do
      @producer = Producer.new(["localhost:9092"],
                               "test_client",
                               :type => :sync,
                               :required_acks => 1)

      messages = [MessageToSend.new("topic_simple_producer_and_consumer", "Hello World")]
      expect(@producer.send_messages(messages)).to eq(true)

      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -2)
      messages = @consumer.fetch
      expect(messages.last.value).to eq("Hello World")

      # Second Batch
      messages = [MessageToSend.new("topic_simple_producer_and_consumer", "Hello World Again")]
      expect(@producer.send_messages(messages)).to eq(true)

      messages = @consumer.fetch
      expect(messages.map(&:value)).to eq(["Hello World Again"])

      # Empty Batch
      messages = @consumer.fetch
      expect(messages.empty?).to eq(true)
    end
  end

  describe "broker that becomes unavailable" do
    it "fails the fetch" do
      @producer = Producer.new(["localhost:9092"],
                               "test_client",
                               :type => :sync)


      messages = [MessageToSend.new("topic_simple_producer_and_consumer", "Hello World")]
      expect(@producer.send_messages(messages)).to eq(true)

      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -2)

      $tc.broker.without_process do
        expect { @consumer.fetch }.to raise_error(Connection::ConnectionFailedError)
      end
    end
  end
end
