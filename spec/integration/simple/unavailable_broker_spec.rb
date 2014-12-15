require 'integration/simple/spec_helper'

RSpec.describe "unavailable broker scenarios:", :type => :request do
  context "producer with a dead broker in bootstrap list" do
    before(:each) do
      @p = NewProducer.new("test", ["localhost:9091","localhost:9092"])
    end

    after(:each) do
      @p.close
    end

    it "succesfully sends a message" do
      expect {
        @p.send_message(MessageToSend.new("test", "hello")).get
      }.to_not raise_error

      pc = PartitionConsumer.new("test_consumer", "localhost",
                                 9092, "test", 0, -2)

      messages = pc.fetch
      expect(messages.last.value).to eq("hello")
    end
  end

  context "producer with required_acks set to 1 and no retries" do
    before(:each) do
      @p = NewProducer.new("test", ["localhost:9092"], :required_acks => 1)
    end

    after(:each) do
      @p.close
    end

=begin
    context "broker stops running" do
      it "fails to send" do
        expect { 
          @p.send_message(MessageToSend.new("test", "hello")).get
          puts "Completed initial send"
        }.to_not raise_error

        $tc.broker.without_process do
          expect {
            @p.send_message(MessageToSend.new("test", "hello-stopped-broker")).get
          }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
        end
      end
    end
  end
=end

    context "broker stops running but starts again" do
      it "failes to send when broker is down" do
        expect {
          metadata = @p.send_message(MessageToSend.new("test", "hello")).get
          expect(metadata.topic).to eq("test")
          expect(metadata.partition).to eq(0)
          expect(metadata.offset).to eq(0)
        }.to_not raise_error

        sent_when_down = nil
        $tc.broker.without_process do
          sent_when_down = @p.send_message(MessageToSend.new("test", "hello again"))
        end

        expect {
          sent_when_down.get
        }.to raise_error(Errors::UnknownTopicOrPartition)
      end
    end
  end

  context "producer with required_acks set to 1 and 3 retries" do
    before(:each) do
      @p = NewProducer.new("test", ["localhost:9092"], required_acks: 1, retires: 300)
    end

    after(:each) do
      @p.close
    end

    context "broker stops running but starts again" do
      it "sends succesfully once broker returns" do
        expect {
          metadata = @p.send_message(MessageToSend.new("test", "hello")).get
          expect(metadata.topic).to eq("test")
          expect(metadata.partition).to eq(0)
          expect(metadata.offset).to eq(0)
        }.to_not raise_error

        sent_when_down = nil
        $tc.broker.without_process do
          sent_when_down = @p.send_message(MessageToSend.new("test", "hello again"))
        end

        metadata = sent_when_down.get
        expect(metadata.topic).to eq("test")
        expect(metadata.partition).to eq(0)
        expect(metadata.offset).to eq(1)
      end
    end
  end

=begin
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
=end
end
