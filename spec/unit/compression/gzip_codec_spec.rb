# encoding: utf-8
require 'spec_helper'

describe Poseidon::Compression::GzipCodec do

  let :data do
    %({"a":"val1"}\n{"a":"val2"}\n{"a":"val3"})
  end

  it "should have an ID" do
    expect(described_class.codec_id).to eq(1)
  end

  it "should compress" do
    compressed = described_class.compress(data)
    expect(compressed.size).to eq(41)
    expect(compressed.encoding).to eq(Encoding::BINARY)
  end

  it "should decompress" do
    original = described_class.decompress(described_class.compress(data))
    expect(original).to eq(data)
    expect(original.encoding).to eq(Encoding::UTF_8)
  end

  it "should decompress unicode messages" do
    str = "\x1F\x8B\b\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\xE3I\x91\xD3|\x19\x18\xFE\x03\x01\x90\xA7Z\xAD\x94\xA8d\xA5\x14Z\x92XP\xEC\xE9\xE3\xE1\xEB\x12Y\xEE\xE8\x98\x16\xA4\xA4\xA3\x94\x04\x14~6}\xE9\xB39k\x94j\xA1Z\x19A\xDAm\f\xD9\xEF\x10\xD0\x1E\x8C\xA6\x1D\x00\x96\x98\x1E\xB9~\x00\x00\x00".force_encoding(Encoding::BINARY)
    buf = Protocol::ResponseBuffer.new(described_class.decompress(str))
    msg = MessageSet.read_without_size(buf).flatten
    expect(msg.size).to eq(2)
    expect(msg[0].value).to eq(%({"a":"UtapsILHMDYwAAfR","b":"日本"}))
  end

end
