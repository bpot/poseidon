require 'spec_helper'

describe MessagesToSendBatch do
  context "messages sent to two different brokers" do
    before(:each) do
      message_conductor = stub('message_conductor')
      message_conductor.stub!(:destination).and_return([0,0],[1,1])
      MessageConductor.stub!(:new).and_return(message_conductor)

      @messages = [
        Message.new(:topic => "topic1", :value => "hi"),
        Message.new(:topic => "topic1", :value => "hi")
      ]
      topics_metadata = stub('topics_metadata').as_null_object
      @batch = MessagesToSendBatch.new(@messages, partitioner = nil, topics_metadata)
    end

    it "returns a couple messages brokers" do
      expect(@batch.messages_for_brokers.size).to eq(2)
    end

    it "has all messages in the returned message brokers" do
      messages = @batch.messages_for_brokers.map(&:messages).flatten
      expect(messages.to_set).to eq(@messages.to_set) 
    end
  end
end
