require 'integration/simple/spec_helper'

RSpec.describe "compression", :type => :request do
  it "roundtrips" do
    i = rand(1000)

    @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                      "test12", 0, -2)

    @producer = NewProducer.new("test_client", ["localhost:9092"], :compression_codec => :gzip)
    message = MessageToSend.new("test12", "Hello World: #{i}")

    @producer.send_messages(messages).get
    sleep 1

    messages = @consumer.fetch
    pp messages
    expect(messages.last.value).to eq("Hello World: #{i}")
  end
end
