require 'spec_helper'

RSpec.describe ProducerCompressionConfig do
  describe "creation" do
    it "raises ArgumentError when codec is unknown" do
      expect { ProducerCompressionConfig.new(:ripple, nil) }.to raise_error(ArgumentError)
    end
  end

  context "no codec set" do
    it "compresses no topics" do
      pcc = ProducerCompressionConfig.new(nil,nil)
      expect(pcc.compression_codec_for_topic("test")).to eq(false)
    end
  end

  describe "none compression codec" do
    it "compresses no topics" do
      pcc = ProducerCompressionConfig.new(:none,nil)
      expect(pcc.compression_codec_for_topic("test")).to eq(false)
    end
  end

  describe "compression codec no topics specified" do
    it "compresses any topic" do
      pcc = ProducerCompressionConfig.new(:gzip,nil)
      expect(pcc.compression_codec_for_topic("test")).to eq(Compression::GzipCodec)
    end
  end

  describe "compression codec set, but only compress 'compressed' topic" do
    it "compresses 'compressed' topic" do
      pcc = ProducerCompressionConfig.new(:gzip, ["compressed"])
      expect(pcc.compression_codec_for_topic("compressed")).to eq(Compression::GzipCodec)
    end

    it "does not compresses 'test' topic" do
      pcc = ProducerCompressionConfig.new(:gzip, ["compressed"])
      expect(pcc.compression_codec_for_topic("test")).to eq(false)
    end
  end
end
