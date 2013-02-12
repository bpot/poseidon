require 'spec_helper'

describe Producer do
  it "requires brokers and client_id" do
    expect { Producer.new }.to raise_error
  end

  it "raises ArgumentError on unknown arguments" do
    expect { Producer.new([],"client_id", :unknown => true) }.to raise_error(ArgumentError)
  end

  it "raises ArgumentError unless brokers is an enumerable" do
    expect { Producer.new("host:port","client_id") }.to raise_error(ArgumentError)
  end

  it "raises ProducerShutdown if we try to send to a shutdown producer" do
    p = Producer.new(["host:port"],"client_id")
    p.shutdown
    expect { p.send_messages([]) }.to raise_error(Errors::ProducerShutdownError)
  end

  it "accepts all options" do
    expect { Producer.new([],"client_id", Producer::OPTION_DEFAULTS.dup) }.not_to raise_error
  end

  describe "sending messages" do
    before(:each) do
      @sync_producer = double('sync_producer').as_null_object
      SyncProducer.stub!(:new).and_return(@sync_producer)

      @producer = Producer.new([], "client_id", :type => :sync)
    end

    it "turns MessagesToSend into Message objects" do
      @sync_producer.should_receive(:send_messages).with([an_instance_of(Message)])

      m = MessageToSend.new("topic", "value")
      @producer.send_messages([m])
    end

    it "raises an ArgumentError if you try to send a single message" do
      expect { @producer.send_messages(MessageToSend.new("topic", "value")) }.to raise_error(ArgumentError)
    end
  end
end
