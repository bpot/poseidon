require 'spec_helper'

describe PartitionConsumer do
  before(:each) do
    @connection = stub('connection')
    Connection.stub!(:new).and_return(@connection)

    offset = Protocol::Offset.new(100)
    partition_offsets = [Protocol::PartitionOffset.new(0, 0, [offset])]
    @offset_response = [Protocol::TopicOffsetResponse.new("test_topic", partition_offsets)]
    @connection.stub(:offset).and_return(@offset_response)
  end

  describe "creation" do
    context "when passed unknown options" do
      it "raises an ArgumentError" do
        expect { PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0,-2, :unknown => true) }.to raise_error(ArgumentError)
      end
    end

    context "when passed an unknown offset" do
      it "raises an ArgumentError" do
        expect { PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0,:coolest_offset) }.to raise_error(ArgumentError)
      end
    end
  end

  describe "next offset" do
    context "when offset is not set" do
      it "resolves offset if it's not set" do
        @connection.should_receive(:offset).and_return(@offset_response)
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, -2)

        pc.next_offset
      end

      it "returns resolved offset" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, -2)
        expect(pc.next_offset).to eq(100)
      end
    end

    context "when offset is set" do
      it "does not resolve it" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, 200)
        pc.next_offset
      end
    end

    context "when call returns an error" do
      it "is raised" do
        @offset_response.first.partition_offsets.first.stub!(:error).and_return(2)
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, -2)

        expect { pc.next_offset }.to raise_error(Errors::InvalidMessage)
      end
    end

    context "when no offset exists" do
      it "sets offset to 0" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, -2)

        @offset_response.first.partition_offsets.first.stub!(:offsets).and_return([])
        expect(pc.next_offset).to eq(0)
      end
    end

    context "when offset is :second_last_offset" do
      it "resolves offset to one less than the server offset" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, :second_latest_offset)
        expect(pc.next_offset).to eq(99)
      end
    end
  end

  describe "fetching messages" do
    before(:each) do
      message_set = MessageSet.new
      message_set << Message.new(:value => "value", :key => "key", :offset => 90)
      partition_fetch_response = Protocol::PartitionFetchResponse.new(0, 0, 100, message_set)
      topic_fetch_response = Protocol::TopicFetchResponse.new('test_topic',
                                                    [partition_fetch_response])
      @response = Protocol::FetchResponse.new(stub('common'), [topic_fetch_response])

      @connection.stub(:fetch).and_return(@response)
      @pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, -2)
    end

    it "returns FetchedMessage objects" do
      expect(@pc.fetch.first.class).to eq(FetchedMessage)
    end

    it "uses object defaults" do
      @connection.should_receive(:fetch).with(10_000, 0, anything)
      @pc.fetch
    end

    context "when options are passed" do
      it "overrides object defaults" do
        @connection.should_receive(:fetch).with(20_000, 0, anything)
        @pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, -2, :max_wait_ms => 20_000)

        @pc.fetch
      end
    end

    context "when call returns an error" do
      it "is raised" do
        pfr = @response.topic_fetch_responses.first.partition_fetch_responses.first
        pfr.stub!(:error).and_return(2)

        expect { @pc.fetch }.to raise_error(Errors::InvalidMessage)
      end
    end

    it "sets the highwater mark" do
      @pc.fetch
      expect(@pc.highwater_mark).to eq(100)
    end

    it "sets the latest offset" do
      @pc.fetch
      expect(@pc.next_offset).to eq(91)
    end
  end
end
