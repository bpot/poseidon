require 'spec_helper'

include Protocol
describe ClusterMetadata do
  describe "populated" do
    before(:each) do
      partitions = [
        PartitionMetadata.new(1, 1, [1,2], [1,2], nil),
        PartitionMetadata.new(2, 2, [2,1], [2,1], nil)
      ]
      topics = [TopicMetadataStruct.new(nil, "test", partitions)]

      brokers = [Broker.new(1, "host1", 1), Broker.new(2, "host2", 2)]

      @mr = MetadataResponse.new(nil, brokers, topics)

      @cm = ClusterMetadata.new
      @cm.update(@mr)
    end

    it "knows when it has metadata for a set of topics" do
      have_metadata = @cm.have_metadata_for_topics?(Set.new(["test"]))
      expect(have_metadata).to eq(true)
    end

    it "knows when it doesn't have metadata for a topic" do
      have_metadata = @cm.have_metadata_for_topics?(Set.new(["test", "no_data"]))
      expect(have_metadata).to eq(false)
    end

    it "provides topic metadata for a set of topics" do
      topic_metadata = @cm.metadata_for_topics(Set.new(["test"]))
      expect(topic_metadata).to eq({ "test" => TopicMetadata.new(@mr.topics.first) })
    end

    it "provides broker information" do
      broker = @cm.broker(1)
      expect(broker).to eq(@mr.brokers.first)
    end
  end
end
