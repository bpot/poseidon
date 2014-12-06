module Poseidon
  # @api private
  class ProducerCompressionConfig
    COMPRESSION_CODEC_MAP = {
      :gzip   => Compression::GzipCodec,
      :snappy => Compression::SnappyCodec,
      :none   => nil
    }

    def initialize(compression_codec, compressed_topics)
      if compression_codec
        unless COMPRESSION_CODEC_MAP.has_key?(compression_codec)
          raise ArgumentError, "Unknown compression codec: '#{compression_codec}' (accepted: #{COMPRESSION_CODEC_MAP.keys.inspect})"
        end
        @compression_codec = COMPRESSION_CODEC_MAP[compression_codec]
      else
        @compression_codec = nil
      end

      if compressed_topics
        @compressed_topics = Set.new(compressed_topics)
      else
        @compressed_topics = nil
      end
    end

    def compression_codec_for_topic(topic)
      return false if @compression_codec.nil?

      if @compressed_topics.nil? || (@compressed_topics && @compressed_topics.include?(topic))
        @compression_codec
      else
        false
      end
    end
  end
end
