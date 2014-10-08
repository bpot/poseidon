module Poseidon
  module Compression
    module GzipCodec
      def self.codec_id
        1
      end

      def self.compress(s)
        io = StringIO.new
        io.set_encoding(Encoding::BINARY)
        gz = Zlib::GzipWriter.new io, Zlib::DEFAULT_COMPRESSION, Zlib::DEFAULT_STRATEGY
        gz.write s
        gz.close
        io.string
      end

      def self.decompress(s)
        io = StringIO.new(s)
        Zlib::GzipReader.new(io).read
      end
    end
  end
end
