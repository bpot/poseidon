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
end
