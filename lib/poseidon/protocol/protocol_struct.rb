module Poseidon
  module Protocol
    class ProtocolStruct < Struct
      class EncodingError < StandardError;end
      class DecodingError < StandardError;end

      def self.new(hash)
        klass = super(*hash.keys)
        klass.type_map = hash
        klass
      end

      def self.type_map=(type_map)
        @type_map = type_map
      end

      def self.type_map
        @type_map 
      end

      def self.prepend_size
        @prepend_size = true
        self
      end

      def self.prepend_crc32
        @prepend_crc32 = true
        self
      end

      def self.truncatable
        @truncatable = true
        self
      end

      def self.prepend_size?
        @prepend_size
      end

      def self.prepend_crc32?
        @prepend_crc32
      end

      def self.truncatable?
        @truncatable
      end

      def self.size_bound_array(member)
        @size_bound_members ||= []
        @size_bound_members  << member
        self
      end

      def self.size_bound_array?(member)
        @size_bound_members ||= []
        @size_bound_members.include?(member)
      end

      # Recursively find all objects with errors
      def objects_with_errors
        children = []
        each_pair do |member, value|
          case value
          when Array
            value.each do |v|
              if v.respond_to?(:objects_with_errors)
                children << v
              end
            end
          else
            if value.respond_to?(:objects_with_errors)
              children << value
            end
          end
        end

        children_with_errors = children.map(&:objects_with_errors).flatten
        if members.include?(:error) && self[:error] != Errors::NO_ERROR_CODE
          children_with_errors + [self]
        else
          children_with_errors
        end
      end

      def raise_error
        raise error_class if error_class
      end

      def error_class
        Errors::ERROR_CODES[self[:error]]
      end

      def raise_error_if_one_exists
        objects_with_errors.each do |object|
          object.raise_error
        end
      end

      def write(buffer)
        maybe_prepend_size(buffer) do
          maybe_prepend_crc32(buffer) do
            each_pair do |member, value|
              begin
                write_member(buffer, member, value)
              rescue
                raise EncodingError, "Error writting #{member} in #{self.class} (#{$!.class}: #{$!.message})"
              end
            end
          end
        end
      end

      def maybe_prepend_size(buffer)
        if self.class.prepend_size?
          buffer.prepend_size do
            yield
          end
        else
          yield
        end
      end

      def maybe_prepend_crc32(buffer)
        if self.class.prepend_crc32?
          buffer.prepend_crc32 do
            yield
          end
        else
          yield
        end
      end

      def write_member(buffer, member, value)
        case type = type_map[member]
        when Array
          buffer.int32(value.size) unless self.class.size_bound_array?(member)
          value.each { |v| write_type(buffer, type.first, v) }
        else
          write_type(buffer, type, value)
        end
      end

      def write_type(buffer, type, value)
        case type
        when Symbol
          buffer.send(type, value)
        else
          value.write(buffer)
        end
      end

      # Populate struct from buffer based on members and their type definition.
      def self.read(buffer)
        s = new
        s.read(buffer)
        s
      end

      def read(buffer)
        if self.class.prepend_size?
          if !have_header?(buffer)
            @truncated = true
            return
          end

          @size = buffer.int32

          if self.class.prepend_crc32?
            @crc32 = buffer.int32
            @computed_crc32 = [Zlib::crc32(buffer.peek(@size-4))].pack("l>").unpack("l>").first
            if @crc32 != @computed_crc32
              @checksum_failed = true
            end
            expected_bytes_remaining = @size - 4
          else
            expected_bytes_remaining = @size
          end

          if self.class.truncatable? && expected_bytes_remaining > buffer.bytes_remaining
            @truncated = true
            return
          end
        end

        members.each do |member|
          begin
            self[member] = read_member(buffer, member)
          rescue DecodingError
            # Just reraise instead of producing a crazy nested exception
            raise
          rescue
            raise DecodingError, "Error while reading #{member} in #{self.class} (#{$!.class}: #{$!.message}))"
          end
        end
      end

      def have_header?(buffer)
        if self.class.truncatable?
          if self.class.prepend_crc32?
            header_bytes = 8
          else
            header_bytes = 4
          end

          return buffer.bytes_remaining >= header_bytes
        else
          return true
        end
      end

      def read_member(buffer, member)
        case type = type_map[member]
        when Array
          if self.class.size_bound_array?(member)
            if @size
              array_buffer = ResponseBuffer.new(buffer.read(@size))
            else
              array_buffer = buffer
            end

            array = []
            while !array_buffer.eof? && (v = read_type(array_buffer, type.first))
              array << v
            end
            array
          else
            buffer.int32.times.map { read_type(buffer, type.first) }
          end
        else
          read_type(buffer, type)
        end
      end

      def read_type(buffer, type)
        case type
        when Symbol
          buffer.send(type)
        else
          type.read(buffer)
        end
      end

      def type_map
        self.class.type_map
      end

      def checksum_failed?
        @checksum_failed
      end

      def truncated?
        @truncated
      end
    end
  end
end
