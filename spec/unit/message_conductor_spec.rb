require 'spec_helper'

describe MessageConductor do
  context "two avialable partitions" do
    before(:each) do
      partitions = [
        Protocol::PartitionMetadata.new(nil, 0, 1, [1,2], [1,2]),
        Protocol::PartitionMetadata.new(nil, 1, 2, [2,1], [2,1])
      ]

      @topics_metadata = {
        "test" => TopicMetadata.new(Protocol::TopicMetadataStruct.new(nil, "test", partitions))
      }
    end

    context "no custom partitioner" do
      before(:each) do
        @mc = MessageConductor.new(@topics_metadata, nil)
      end

      context "for unkeyed messages" do
        it "round robins which partition the message should go to" do
          [0,1,0,1].each do |destination|
            expect(@mc.destination("test").first).to eq(destination)
          end
        end

        context "unknown topic" do
          it "returns -1 for broker and partition" do
            expect(@mc.destination("no_exist")).to eq([-1,-1])
          end
        end
      end

      context "keyed message" do
        it "sends the same keys to the same destinations" do
          keys = 1000.times.map { rand(500).to_s }
          key_destinations = {}

          keys.sort_by { rand }.each do |k|
            partition,broker = @mc.destination("test", k)

            key_destinations[k] ||= []
            key_destinations[k].push([partition,broker])
          end

          expect(key_destinations.values.all? { |destinations| destinations.uniq.size == 1 }).to eq(true)
        end
      end
    end

    context "custom partitioner" do
      before(:each) do
        partitioner = Proc.new { |key, count| key.split("_").first.to_i % count }
        @mc = MessageConductor.new(@topics_metadata, partitioner)
      end

      it "obeys custom partitioner" do
        expect(@mc.destination("test", "2_hello").first).to eq(0)
        expect(@mc.destination("test", "3_hello").first).to eq(1)
      end
    end

    context "broken partitioner" do
      before(:each) do
        partitioner = Proc.new { |key, count| count + 1 }
        @mc = MessageConductor.new(@topics_metadata, partitioner)
      end

      it "raises InvalidPartitionError" do
        expect{@mc.destination("test", "2_hello").first}.to raise_error(Errors::InvalidPartitionError)
      end
    end
  end

  context "two partitions, one is unavailable" do
    before(:each) do
      partitions = [
        Protocol::PartitionMetadata.new(nil, 0, 1, [1,2], [1,2]),
        Protocol::PartitionMetadata.new(nil, 1, nil, [2,1], [2,1])
      ]

      @topics_metadata = {
        "test" => TopicMetadata.new(Protocol::TopicMetadataStruct.new(nil, "test", partitions))
      }
      @mc = MessageConductor.new(@topics_metadata, nil)
    end

    context "keyless message" do
      it "is never sent to an unavailable partition" do
        10.times do |destination|
          expect(@mc.destination("test").first).to eq(0)
        end
      end
    end

    context "keyed message" do
      it "is sent to unavailable partition" do
        destinations = Set.new
        100.times do |key|
          destinations << @mc.destination("test",key.to_s).first
        end
        expect(destinations).to eq(Set.new([0,1]))
      end
    end
  end

  context "no available partitions" do
    before(:each) do
      partitions = [
        Protocol::PartitionMetadata.new(nil, 0, nil, [1,2], [1,2]),
        Protocol::PartitionMetadata.new(nil, 1, nil, [2,1], [2,1])
      ]

      @topics_metadata = {
        "test" => TopicMetadata.new(Protocol::TopicMetadataStruct.new(nil, "test", partitions))
      }
      @mc = MessageConductor.new(@topics_metadata, nil)
    end

    context "keyless message" do
      it "return -1 for broker and partition" do
        expect(@mc.destination("test").first).to eq(-1)
      end
    end

    context "keyed message" do
      it "returns a valid partition and -1 for broker" do
        partition_id, broker_id = @mc.destination("test", "key")
        expect(partition_id).to_not eq(-1)
        expect(broker_id).to eq(-1)
      end
    end
  end
end
