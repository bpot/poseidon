require 'integration/multiple_brokers/spec_helper'

describe "producer handles rebalancing" do
  before(:each) do
    # autocreate the topic by asking for information about it
    @c = Connection.new("localhost", 9093, "metadata_fetcher", 10_000)
    @c.topic_metadata(["failure_spec"])
    sleep 1
  end

  def current_leadership_mapping(c)
    metadata = c.topic_metadata(["failure_spec"])
    topic_metadata = metadata.topics.find { |t| t.name == "failure_spec" }
    (0..2).map { |p| topic_metadata.partition_leader(p) }
  end

  it "produces a bunch of messages and consumes all without error" do
    @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "test",
                     :required_acks => -1)

    1.upto(25) do |n|
      @p.send_messages([MessageToSend.new("failure_spec", n.to_s)])
    end

    # The goal here is to have the producer attempt to send messages
    # to a broker which is no longer the leader for the partition.
    #
    # We accomplish this by turning off a broker which causes leadership
    # to failover. Then we turn that broker back on and begin sending
    # messages. While sending messages, the kafka cluster should rebalance
    # the partitions causing leadership to switch back to the original
    # broker in the midst of messages being sent.
    #
    # We compare leadership before and after the message sending period
    # to make sure we were successful.
    $tc.stop_first_broker
    sleep 30
    SPEC_LOGGER.info "Pre start #{current_leadership_mapping(@c).inspect}"
    $tc.start_first_broker

    pre_send_leadership = current_leadership_mapping(@c)
    SPEC_LOGGER.info "Pre send #{pre_send_leadership.inspect}"
    26.upto(50) do |n|
      sleep 0.5
      @p.send_messages([MessageToSend.new("failure_spec", n.to_s)])
    end
    post_send_leadership = current_leadership_mapping(@c)
    SPEC_LOGGER.info "Post send #{post_send_leadership.inspect}"

    expect(pre_send_leadership).to_not eq(post_send_leadership)

    messages = []
    0.upto(2) do |partition|
      consumer = PartitionConsumer.consumer_for_partition("consumer_failure_spect",
                                                          ["localhost:9092","localhost:9093","localhost:9094"],
                                                          "failure_spec",
                                                          partition,
                                                          :earliest_offset)
      while (fetched = consumer.fetch).any?
        messages.push(*fetched)
      end
    end

    expect(messages.size).to eq(50)
    expect(messages.map { |m| m.value.to_i }.sort).to eq((1..50).to_a)
  end
end
