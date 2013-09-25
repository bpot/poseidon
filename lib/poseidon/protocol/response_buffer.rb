module Poseidon
  module Protocol
    class ResponseBuffer
      def initialize(response)
        @s = response
        @pos = 0
      end

      def int8
        byte = @s.slice(@pos, 1).unpack("C").first
        @pos += 1
        byte
      end

      def int16
        short = @s.slice(@pos, 2).unpack("s>").first
        @pos += 2
        short
      end

      def int32
        int = @s.slice(@pos, 4).unpack("l>").first
        @pos += 4
        int
      end

      def int64
        long = @s.slice(@pos, 8).unpack("q>").first
        @pos += 8
        long
      end

      def string
        len = int16
        string = @s.slice(@pos, len)
        @pos += len
        string
      end

      def read(bytes)
        data = @s.slice(@pos, bytes)
        @pos += bytes
        data
      end

      def peek(bytes)
        @s.slice(@pos, bytes)
      end

      def bytes
        n = int32
        if n == -1
          return nil
        else
          read(n)
        end
      end

      def bytes_remaining
        @s.bytesize - @pos
      end

      def eof?
        @pos == @s.bytesize
      end

      def to_s
        @s
      end
    end
  end
end
