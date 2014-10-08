require 'spec_helper'

RSpec.describe FetchedMessage do
  it "provides access to topic,value,key,offset" do
    mts = FetchedMessage.new("hello_topic", "Hello World", "key", 0)
    expect(mts.topic).to eq("hello_topic")
    expect(mts.value).to eq("Hello World")
    expect(mts.key).to eq("key")
    expect(mts.offset).to eq(0)
  end
end
