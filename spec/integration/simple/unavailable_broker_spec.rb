require 'integration/simple/spec_helper'

describe "unavailable broker scenarios:" do
  context "producer with a dead broker in bootstrap list" do
    before(:each) do
      @p = Producer.new(["localhost:9091","localhost:9092"], "test")
    end

    it "succesfully sends a message" do
      expect(@p.send_messages([MessageToSend.new("test", "hello")])).to eq(true)

      pc = PartitionConsumer.new("test_consumer", "localhost",
                                 9092, "test", 0, -2)

      messages = pc.fetch
      expect(messages.last.value).to eq("hello")
    end
  end

  context "producer with required_acks set to 1" do
    before(:each) do
      @p = Producer.new(["localhost:9092"], "test", :required_acks => 1)
    end

    context "broker stops running" do
      it "fails to send" do
        expect(@p.send_messages([MessageToSend.new("test", "hello")])).to eq(true)

        $tc.broker.without_process do
          expect {
            @p.send_messages([MessageToSend.new("test", "hello")])
          }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
        end
      end
    end

    context "broker stops running but starts again" do
      it "sends succesfully once broker returns" do
        expect(@p.send_messages([MessageToSend.new("test", "hello")])).to eq(true)

        $tc.broker.without_process do
          expect {
            @p.send_messages([MessageToSend.new("test", "hello")])
          }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
        end

        expect(@p.send_messages([MessageToSend.new("test", "hello")])).to eq(true)
      end
    end
  end

  context "producer with required_acks set to 0" do
    before(:each) do
      @p = Producer.new(["localhost:9092"], "test", :required_acks => 0)
    end

    context "broker stops running" do
      it "fails to send" do
        expect(@p.send_messages([MessageToSend.new("test", "hello_a")])).to eq(true)

        $tc.broker.without_process do
          @p.send_messages([MessageToSend.new("test", "hello_b")])
          expect {
            @p.send_messages([MessageToSend.new("test", "hello_b")])
          }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
        end
      end
    end
  end
end
