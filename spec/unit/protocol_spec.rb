require 'spec_helper'
include Protocol
describe RequestCommon do
  it "roundtrips" do
    rc = RequestCommon.new(0,1,2,"client_id")

    req_buffer = RequestBuffer.new
    rc.write(req_buffer)

    resp_buffer = ResponseBuffer.new(req_buffer.to_s)
    rc_roundtrip = RequestCommon.read(resp_buffer)

    expect(rc).to eq(rc_roundtrip)
  end
end

describe MetadataRequest do
  it "roundtrips" do
    rc = RequestCommon.new(0,1,2,"client_id")
    mr = MetadataRequest.new(rc, ["topic1","topic2"])

    req_buffer = RequestBuffer.new
    mr.write(req_buffer)

    resp_buffer = ResponseBuffer.new(req_buffer.to_s)
    mr_roundtrip = MetadataRequest.read(resp_buffer)

    expect(mr).to eq(mr_roundtrip)
  end
end

describe "objects with errors" do
  it "returns objects that have errors" do
    message_set = MessageSet.new
    message_set << Message.new(:value => "value", :key => "key")
    partition_fetch_response = PartitionFetchResponse.new(0, 5, 100, message_set)
    topic_fetch_response = TopicFetchResponse.new('test_topic',
                                                  [partition_fetch_response])
    response = FetchResponse.new(double('common'), [topic_fetch_response])

    expect(response.objects_with_errors).to eq([partition_fetch_response])
  end

  it "raises error when asked" do
    message_set = MessageSet.new
    message_set << Message.new(:value => "value", :key => "key")
    partition_fetch_response = PartitionFetchResponse.new(0, 5, 100, message_set)
    topic_fetch_response = TopicFetchResponse.new('test_topic',
                                                  [partition_fetch_response])
    response = FetchResponse.new(double('common'), [topic_fetch_response])

    expect { response.raise_error_if_one_exists }.to raise_error
  end
end
