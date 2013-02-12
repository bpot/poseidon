module Poseidon
  # @api private
  module Compression
    class UnrecognizedCompressionCodec < StandardError; end

    require "poseidon/compression/gzip_codec"
    require "poseidon/compression/snappy_codec"

    CODECS = {
      #0 => no codec
      1 => GzipCodec,
      2 => SnappyCodec
    }

    # Fetches codec module for +codec_id+
    # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Compression
    #
    # @param [Integer] codec_id codec's as defined by the Kafka Protocol
    # @return [Module] codec module for codec_id
    #
    # @private
    def self.find_codec(codec_id)
      codec = CODECS[codec_id]
      if codec.nil?
        raise UnrecognizedCompressionCodec, codec_id
      end
      codec
    end
  end
end
