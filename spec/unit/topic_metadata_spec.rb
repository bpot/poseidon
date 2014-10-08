require 'spec_helper'

RSpec.describe TopicMetadata do
  context "encoding" do
    it "roundtrips" do
      partition_metadata = Protocol::PartitionMetadata.new(0, 0, 0, [0], [0])
      partitions = [partition_metadata]
      tm = TopicMetadata.new(Protocol::TopicMetadataStruct.new(0, "topic", partitions))

      request_buffer = Protocol::RequestBuffer.new
      tm.write(request_buffer)

      response_buffer = Protocol::ResponseBuffer.new(request_buffer.to_s)
      expect(TopicMetadata.read(response_buffer)).to eq(tm)
    end
  end

  it 'determines leader for a partition' do
    partition_metadata = Protocol::PartitionMetadata.new(0, 0, 0, [0], [0])
    partitions = [partition_metadata]
    tm = TopicMetadata.new(Protocol::TopicMetadataStruct.new(0, "topic", partitions))

    expect(tm.partition_leader(0)).to eq(0)
  end
end
