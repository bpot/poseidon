require 'spec_helper'

describe MessageToSend do
  it "provides access to topic,value,key" do
    mts = MessageToSend.new("hello_topic", "Hello World", "key")
    expect(mts.topic).to eq("hello_topic")
    expect(mts.value).to eq("Hello World")
    expect(mts.key).to eq("key")
  end
end
