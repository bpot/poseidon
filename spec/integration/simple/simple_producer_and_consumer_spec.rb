require 'integration/simple/spec_helper'

describe "simple producer and consumer" do

  describe "writing and consuming one topic" do
    it "works" do
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
  end
end
