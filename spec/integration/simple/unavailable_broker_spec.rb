require 'integration/simple/spec_helper'

describe "unavailble broker" do
  context "producer with a dead broker in bootstrap list" do
    before(:each) do
      @p = Producer.new(["localhost:9091","localhost:9092"], "test")
    end

    it "succesfully sends a message" do
      @p.send_messages([MessageToSend.new("test", "hello")])

      pc = PartitionConsumer.new("test_consumer", "localhost",
                                 9092, "test", 0, -2)

      messages = pc.fetch
      expect(messages.first.value).to eq("hello")
    end
  end
end
