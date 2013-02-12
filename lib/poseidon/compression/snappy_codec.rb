module Poseidon
  module Compression
    module SnappyCodec
      def self.codec_id
        2
      end

      def self.compress(s)
        raise "Unimplemented"
      end

      def self.decompress(s)
        raise "Unimplemented"
      end
    end
  end
end
