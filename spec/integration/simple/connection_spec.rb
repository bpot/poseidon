require 'integration/simple/spec_helper'

include Protocol
RSpec.describe Connection, :type => :request do
  before(:each) do
    @connection = Connection.new("localhost", 9092, "test", 10_000)
  end

  it 'sends and parses topic metadata requests' do
    @connection.topic_metadata(["test2"])
  end

  it 'sends and parsers produce requests' do
    message = MessageStruct.new(0, 0, nil, "hello")
    message_with_offset = MessageWithOffsetStruct.new(0, message)
    message_set = MessageSetStruct.new([message_with_offset])
    messages_for_partitions = [MessagesForPartition.new(0,message_set)]
    messages_for_topics = [MessagesForTopic.new("test2",messages_for_partitions)]
    @connection.produce(1, 10_000, messages_for_topics)
  end

  it 'sends and parsers fetch requests' do
    partition_fetches = [PartitionFetch.new(0,0,1024*1024)]
    topic_fetches = [TopicFetch.new("test2", partition_fetches)]
    @connection.fetch(1000, 0, topic_fetches)
  end

  it 'sends and parsers offset requests' do
    partition_offset_requests = [PartitionOffsetRequest.new(0,-1,1000)]
    offset_topic_requests = [TopicOffsetRequest.new("test2", partition_offset_requests)]
    @connection.offset(offset_topic_requests)
  end
end
