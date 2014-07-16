require 'integration/multiple_brokers/spec_helper'

describe "round robin sending" do
  describe "with small message batches" do
    it "evenly distributes messages across brokers" do
      c = Connection.new("localhost", 9092, "metadata_fetcher")
      md = c.topic_metadata(["test"])
      sleep 1
      md = c.topic_metadata(["test"])

      test_topic = md.topics.first

      consumers = test_topic.send(:partitions).map do |partition|
        leader_id = partition.leader
        broker = md.brokers.find { |b| b.id == leader_id }
        PartitionConsumer.new("test_consumer_#{partition.id}", broker.host,
                              broker.port, "test", partition.id, -1)
      end

      # Update offsets to current position before adding test messages
      consumers.each do |c|
        c.fetch
      end

      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "test",
                       :required_acks => 1)
      24.times do
        @p.send_messages([MessageToSend.new("test", "hello")])
      end

      sleep 5

      consumers.each do |c|
        messages = c.fetch
        expect(messages.size).to eq(8)
      end
    end
  end
end
