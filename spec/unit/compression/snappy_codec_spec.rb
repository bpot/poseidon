require 'spec_helper'

describe Poseidon::Compression::SnappyCodec do

  let :data do
    %({"a":"val1"}\n{"a":"val2"}\n{"a":"val3"})
  end

  it "should have an ID" do
    expect(described_class.codec_id).to eq(2)
  end

  it "should compress" do
    compressed = described_class.compress(data)
    expect(compressed.size).to eq(34)
    expect(compressed.encoding).to eq(Encoding::BINARY)
  end

  it "should decompress" do
    original = described_class.decompress(described_class.compress(data))
    expect(original).to eq(data)
  end

  it "should decompress streams" do
    str = "\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x1E#\x00\x00\x19\x01\\\x17\x8B\xA7x\xB9\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00\tPLAINDATA".force_encoding(Encoding::BINARY)
    buf = Protocol::ResponseBuffer.new(described_class.decompress(str))
    msg = MessageSet.read_without_size(buf).flatten
    expect(msg.size).to eq(1)
    expect(msg[0].value).to eq("PLAINDATA")
  end
end
