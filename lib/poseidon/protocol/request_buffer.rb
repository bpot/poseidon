module Poseidon
  module Protocol
    # RequestBuffer allows you to build a Binary string for API requests
    #
    # API parallels the primitive types described on the wiki, with some
    # sugar for prepending message sizes and checksums.
    # (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProtocolPrimitiveTypes) 
    class RequestBuffer
      def initialize
        @s = ''.encode("ASCII-8BIT")
      end

      def append(string)
        string = string.dup
        string.force_encoding("ASCII-8BIT")
        @s << string
        nil
      end

      def int8(int8)
        append([int8].pack("C"))
      end

      def int16(int16)
        append([int16].pack("s>"))
      end

      def int32(int32)
        append([int32].pack("l>"))
      end

      def int64(int64)
        append([int64].pack("q>"))
      end

      # Add a string
      # 
      # @param [String] string
      def string(string)
        if string.nil?
          int16(-1)
        else
          int16(string.bytesize)
          append(string)
        end
      end

      def bytes(string)
        if string.nil?
          int32(-1)
        else
          int32(string.bytesize)
          append(string)
        end
      end

      def prepend_crc32
        checksum_pos = @s.bytesize
        @s += " "
        yield
        @s[checksum_pos] = [Zlib::crc32(@s[(checksum_pos+1)..-1])].pack("N")
        nil
      end

      def prepend_size
        size_pos = @s.bytesize
        @s += " "
        yield
        @s[size_pos] = [(@s.bytesize-1) - size_pos].pack("N")
        nil
      end

      def to_s
        @s
      end
    end
  end
end
