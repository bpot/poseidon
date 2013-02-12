module Poseidon
  # @api private
  class MessageSet
    # Build a message set object from a binary encoded message set
    #
    # @param [String] string binary encoded message set
    # @return [MessageSet]


    def self.read(buffer)
      ms = MessageSet.new
      ms.struct = Protocol::MessageSetStructWithSize.read(buffer)
      ms
    end

    def self.read_without_size(buffer)
      ms = MessageSet.new
      ms.struct = Protocol::MessageSetStruct.read(buffer)
      ms
    end

    attr_accessor :struct
    def initialize(messages = [])
      self.struct = Protocol::MessageSetStructWithSize.new(messages)
    end

    def ==(other)
      eql?(other)
    end

    def eql?(other)
      struct.eql?(other.struct)
    end

    def objects_with_errors
      struct.objects_with_errors
    end

    def write(buffer)
      struct.write(buffer)
    end

    def <<(message)
      struct.messages << message
    end

    def messages
      struct.messages
    end

    def compress(codec)
      MessageSet.new([to_compressed_message(codec)])
    end

    # Builds an array of Message objects from the MessageStruct objects. 
    # Decompressing messages if necessary.
    #
    # @return [Array<Message>]
    def flatten
      messages = struct.messages.map do |message| 
        if message.compressed?
          s = message.decompressed_value
          MessageSet.read_without_size(Protocol::ResponseBuffer.new(s)).flatten
        else
          message
        end
      end.flatten
    end

    private
    def to_compressed_message(codec)
      buffer = Protocol::RequestBuffer.new
      struct.write(buffer)

      value = codec.compress(buffer.to_s[4..-1])
      Message.new(:value => value, :attributes => codec.codec_id)
    end

  end
end
