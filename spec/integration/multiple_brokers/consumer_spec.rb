require 'integration/multiple_brokers/spec_helper'

describe "consuming with multiple brokers" do
  before(:each) do
    # autocreate the topic by asking for information about it
    c = Connection.new("localhost", 9092, "metadata_fetcher")
    md = c.topic_metadata(["test"])
    sleep 1
  end

  it "finds the lead broker for each partition" do
    brokers = Set.new
    0.upto(2) do |partition|
      pc = PartitionConsumer.consumer_for_partition("test_client",
                                                    ["localhost:9092"],
                                                    "test", partition,
                                                    :earliest_offset)

      brokers.add("#{pc.host}:#{pc.port}")
    end
    expect(brokers.size).to eq(3)
  end

  it "consumes from all partitions" do
    @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "test",
                     :required_acks => 1)

    msgs = 24.times.map { |n| "hello_#{n}" }
    msgs.each do |msg|
      @p.send_messages([MessageToSend.new("test", msg)])
    end

    fetched_messages = []
    0.upto(2) do |partition|
      pc = PartitionConsumer.consumer_for_partition("test_client",
                                                    ["localhost:9092"],
                                                    "test", partition,
                                                    :earliest_offset)
      fetched_messages.push(*pc.fetch)
    end
    expect(fetched_messages.map(&:value).sort).to eq(msgs.sort)
  end
end
