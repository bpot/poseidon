require 'spec_helper'

RSpec.describe MessageSet do
  describe "converting to a compressed message" do
    before(:each) do
      ms = MessageSet.new([Message.new(:value => "I will be compressed", :topic => "test")])

      @compressed_message_set = ms.compress(Compression::GzipCodec)
    end

    it "contains a compressed message" do
      expect(@compressed_message_set.messages.first.compressed?).to eq(true)
    end

    it "can be decompressed and reconstituted" do
      expect(@compressed_message_set.flatten.first.value).to eq("I will be compressed")
    end
  end

  describe "adding messages" do
    it "adds the message to the struct" do
      m = Message.new(:value => "sup", :topic => "topic")
      ms = MessageSet.new
      ms << m
      expect(ms.struct.messages.first).to eq(m)
    end
  end

  describe "encoding" do
    it "round trips" do
      m = Message.new(:value => "sup", :key => "keyz", :topic => "hello")
      ms = MessageSet.new
      ms << m

      request_buffer = Protocol::RequestBuffer.new
      ms.write(request_buffer)

      response_buffer = Protocol::ResponseBuffer.new(request_buffer.to_s)
      expect(MessageSet.read(response_buffer)).to eq(ms)
    end
  end
end
