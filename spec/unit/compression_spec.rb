require 'spec_helper'

RSpec.describe Compression do
  it 'returns GzipCompessor for codec_id of 1' do
    codec = Compression.find_codec(1)
    expect(codec).to eq(Compression::GzipCodec)
  end

  it 'returns SnappyCompessor for codec_id of 2' do
    codec = Compression.find_codec(2)
    expect(codec).to eq(Compression::SnappyCodec)
  end

  it 'raises UnrecognizedCompressionCodec for codec_id of 3' do
    expect { Compression.find_codec(3) }.to raise_error(Compression::UnrecognizedCompressionCodec)
  end
end
