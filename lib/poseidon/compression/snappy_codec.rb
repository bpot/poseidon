module Poseidon
  module Compression
    module SnappyCodec
      def self.codec_id
        2
      end

      def self.compress(s)
        check!
        Snappy.deflate(s)
      end

      def self.decompress(s)
        check!
        Snappy.inflate(s)
      end

      def self.check!
        @checked ||= begin
          require 'snappy'
          true
        rescue LoadError
          raise "Snappy compression is not available, please install the 'snappy' gem"
        end
      end

    end
  end
end
