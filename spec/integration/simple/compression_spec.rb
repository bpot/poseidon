require 'integration/simple/spec_helper'

describe "compression" do
  it "roundtrips" do
    i = rand(1000)

    @consumer = PartitionConsumer.new("test_consumer", "localhost", 9092,
                                      "test12", 0, -2)

    @producer = Producer.new(["localhost:9092"],
                             "test_client",
                             :type => :sync,
                             :compression_codec => :gzip)
    messages = [MessageToSend.new("test12", "Hello World: #{i}")]
    
    expect(@producer.send_messages(messages)).to eq(true)
    sleep 1
    messages = @consumer.fetch
    expect(messages.last.value).to eq("Hello World: #{i}")
  end
end
