require 'spec_helper'

include Protocol
describe ClusterMetadata do
  describe "populated" do
    before(:each) do
      partitions = [
        PartitionMetadata.new(0, 1, 1, [1,2], [1,2]),
        PartitionMetadata.new(0, 2, 2, [2,1], [2,1])
      ]
      topics = [TopicMetadata.new(TopicMetadataStruct.new(0, "test", partitions))]

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
      expect(topic_metadata).to eq({ "test" => @mr.topics.first })
    end

    it "provides broker information" do
      broker = @cm.broker(1)
      expect(broker).to eq(@mr.brokers.first)
    end

    it "provides the lead broker for a partition" do
      expect(@cm.lead_broker_for_partition("test",1).id).to eq(1)
      expect(@cm.lead_broker_for_partition("test",2).id).to eq(2)
    end
  end
end
