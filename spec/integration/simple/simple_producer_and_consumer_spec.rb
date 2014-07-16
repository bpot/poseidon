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

      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -1)

      # Read up to the end of the current messages (if there are any)
      begin
        @consumer.fetch
      rescue Errors::UnknownTopicOrPartition
      end

      # First Batch
      messages = [MessageToSend.new("topic_simple_producer_and_consumer", "Hello World")]
      expect(@producer.send_messages(messages)).to eq(true)

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

    it "waits for messages" do
      # Create topic
      @c = Connection.new("localhost", 9092, "metadata_fetcher")
      @c.topic_metadata(["simple_wait_test"])

      sleep 5
      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "simple_wait_test", 0, :earliest_offset,
                                        :max_wait_ms => 2500)

      require 'benchmark'
      n = Benchmark.realtime do
        @consumer.fetch
      end
      expect(n).to be_within(0.25).of(2.5)
    end

    # Not sure what's going on here, will revisit.
=begin
    it "fetches larger messages with a larger max bytes size" do
      @producer = Producer.new(["localhost:9092"],
                                "test_client",
                                :type => :sync,
                                :required_acks => 1)

      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -2)
      
      messages = []
      2000.times do
        messages << MessageToSend.new("topic_simple_producer_and_consumer",'KcjNyFBtqfSbpwjjcGKckMKLUCWz83IVcp21C8FQzs8JJKKTTrc4OLxSjLpYc5z7fsncX59te2cBn0sWDRaYmRuZyttRMLMHvXrM5o3QReKPIYUKzVCFahC4cb3Ivcbb5ZuS98Ohnb7Io42Bz9FucXwwGkQyFhJwyn3nD3BYs5r8TZM8Q76CGR2kTH1rjnFeB7J3hrRKukztxCrDY3smrQE1bbVR80IF3yWlhzkdfv3cpfwnD0TKadtt21sFJANFmORAJ0HKs6Z2262hcBQyF7WcWypC2RoLWVgKVQxbouVUP7yV6YYOAQEevYrl9sOB0Yi6h1mS8fTBUmRTmWLqyl8KzwbnbQvmCvgnX26F5JEzIoXsVaoDT2ks5eep9RyE1zm5yPtbYVmd2Sz7t5ru0wj6YiAmbF7Xgiw2l4VpNOxG0Ec6rFxXRXs0bahyBd2YtxpGyZBeruIK1RAN4n0t97xVXgZG5CGoVhL1oRDxw2pTbwEO1cvwHiiYXpXSqaxF7G9kiiPsQt24Vu7chXrJT7Xqv4RIg1aOT5Os5JVlISaJCmx8ZLtbC3OjAdGtF1ZkDuUeQHHohqeKh0qBJjw7Rv1oSDwcM0MRazjF36jijpYg26Qml9lSEnGYIFLQWHVDWKqqhl2GIntjxDXn1IyI')
      end
      expect(@producer.send_messages(messages)).to eq(true)
      
      messages = @consumer.fetch
      expect(messages.length).to be > 2 
      
      @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                        "topic_simple_producer_and_consumer", 0, -2)
      messages = @consumer.fetch(:max_bytes => 1400000)
      expect(messages.length).to be > 2
    end 
=end
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
