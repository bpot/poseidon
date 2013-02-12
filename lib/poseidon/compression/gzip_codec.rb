module Poseidon
  module Compression
    module GzipCodec
      def self.codec_id
        1
      end

      def self.compress(s)
        io = StringIO.new
        io.set_encoding("ASCII-8BIT")
        gz = Zlib::GzipWriter.new io, nil, nil, :encoding => "ASCII-8BIT"
        gz.write s
        gz.close
        io.string
      end

      def self.decompress(s)
        io = StringIO.new(s)
        Zlib::GzipReader.new(io, :encoding => "ASCII-8BIT").read
      end
    end
  end
end
