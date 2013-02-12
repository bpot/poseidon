module Poseidon
  # @api private
  class CompressedValue
    def initialize(value, codec_id)
      @value = value
      @codec_id = codec_id
    end

    # Decompressed value
    #
    # Raises ??? if the compression codec is uknown
    #
    # @return [String] decompressed value
    def decompressed
      @decompressed ||= decompress
    end

    def compression_codec
      Compression.find_codec(codec_id)
    end
    private
  end
end
