require 'spec_helper'
include Poseidon::Protocol

describe ProduceResult do
  context "successful result" do
    before(:each) do
      @messages = [mock('message')]
      partition_response = ProducePartitionResponse.new(1, 0, 10)
      topic_response = ProduceTopicResponse.new("topic", [partition_response])
      @pr = ProduceResult.new(
        topic_response,
        partition_response,
        @messages
      )
    end

    it "is success?" do
      expect(@pr.success?).to eq(true)
    end

    it "has no error" do
      expect(@pr.error).to eq(nil)
    end

    it "did not timeout" do
      expect(@pr.timeout?).to eq(false)
    end

    it "provides topic" do
      expect(@pr.topic).to eq("topic")
    end

    it "provides partition" do
      expect(@pr.partition).to eq(1)
    end

    it "provides offset" do
      expect(@pr.offset).to eq(10)
    end

    it "provides messages" do
      expect(@pr.messages).to eq(@messages)
    end
  end

  context "failed result" do
    before(:each) do
      @messages = [mock('message')]
      partition_response = ProducePartitionResponse.new(1, 2, -1)
      topic_response = ProduceTopicResponse.new("topic", [partition_response])
      @pr = ProduceResult.new(
        topic_response,
        partition_response,
        @messages
      )
    end

    it "is success?" do
      expect(@pr.success?).to eq(false)
    end

    it "has error" do
      expect(@pr.error).to eq(Poseidon::Errors::InvalidMessage)
    end

    it "did not timeout" do
      expect(@pr.timeout?).to eq(false)
    end
  end

  context "timedout" do
    before(:each) do
      @messages = [mock('message')]
      partition_response = ProducePartitionResponse.new(1, 7, -1)
      topic_response = ProduceTopicResponse.new("topic", [partition_response])
      @pr = ProduceResult.new(
        topic_response,
        partition_response,
        @messages
      )
    end

    it "is success?" do
      expect(@pr.success?).to eq(false)
    end

    it "has error" do
      expect(@pr.error).to eq(Poseidon::Errors::RequestTimedOut)
    end

    it "did timeout" do
      expect(@pr.timeout?).to eq(true)
    end
  end
end
