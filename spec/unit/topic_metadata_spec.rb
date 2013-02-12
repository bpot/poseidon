require 'spec_helper'

describe TopicMetadata do
  context "encoding" do
    it "roundtrips" do
      partition_metadata = PartitionMetadata.new(0, 0, 0, [0], [0])
      partitions = [partition_metadata]
      tm = TopicMetadata.new(TopicMetadataStruct.new(0, "topic", partitions))

      request_buffer = RequestBuffer.new
      tm.write(request_buffer)

      response_buffer = ResponseBuffer.new(request_buffer.to_s)
      expect(TopicMetadata.read(response_buffer)).to eq(tm)
    end
  end
end
