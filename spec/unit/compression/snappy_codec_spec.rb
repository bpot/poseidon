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

  it "should decompress bulk streams" do
    str = "\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\xCA\xE8\x04\x00\x00\x19\x01\xA0L`\x9E\xD4(\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00>{\"a\":\"UtaaKYLHMCwiAA-l\",\"bF\x17\x00Dm\",\"c\":1389795881}\rW \x01\x00\x00\x00Ln\x14\x98\xA8zX\x00\x00nVX\x00\x00o^X\x00\x00\x02\x01X\b3\xF1\e~\xB0\x00\x00pVX\x00\x00q^X\x00\x00\x03\x01X\b.\xE5\x82~X\x00\x00tVX\x00\x00u^X\x00\x00\x04\x01X\b o\xCE~\b\x01\x00vVX\x00\x00w^X\x00\x00\x05\x01X\f\t\xD8)(z`\x01\x00xVX\x00\x00y^X\x00\x00\x06\x01X\f@\bf\xA6zX\x00\x00zVX\x00\x000BX\x00".force_encoding(Encoding::BINARY)
    buf = Protocol::ResponseBuffer.new(described_class.decompress(str))
    msg = MessageSet.read_without_size(buf).flatten
    expect(msg.size).to eq(7)
    expect(msg[0].value).to eq(%({"a":"UtaaKYLHMCwiAA-l","b":"UtaaKYLHMCwiAA-m","c":1389795881}))
  end

  it "should unicode messages" do
    str = "\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00:?\x00\x00\x19\x01\xCC3\xBA?\x91\xFA\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00%{\"a\":\"UtaitILHMDAAAAfU\",\"b\":\"\xE6\x97\xA5\xE6\x9C\xAC\"}".force_encoding(Encoding::BINARY)
    buf = Protocol::ResponseBuffer.new(described_class.decompress(str))
    msg = MessageSet.read_without_size(buf).flatten
    expect(msg.size).to eq(1)
    expect(msg[0].value).to eq(%({"a":"UtaitILHMDAAAAfU","b":"日本"}))
  end

end
