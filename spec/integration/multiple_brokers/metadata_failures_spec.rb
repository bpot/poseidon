require 'integration/multiple_brokers/spec_helper'

describe "handling failures" do
  describe "metadata failures" do
    before(:each) do
      @messages_to_send = [
        MessageToSend.new("topic1", "hello"),
        MessageToSend.new("topic2", "hello")
      ]
    end

    describe "unable to connect to brokers" do
      before(:each) do
        @p = Producer.new(["localhost:1092","localhost:1093","localhost:1094"], "producer")
      end

      it "triggers callback failures for both topics" do
        expect {
          @p.send_messages(@messages_to_send)
        }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
      end
    end
  end

  describe "unknown topic" do
    it "receives error callback" do
      pending "need a way to turn off auto-topic creation just for this test"
      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "producer")

      expect {
        @p.send_messages([MessageToSend.new("imnothere", "hello")])
      }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
    end
  end
end
