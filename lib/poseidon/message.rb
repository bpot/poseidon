module Poseidon
  # The Message class is used by both Producer and Consumer classes.
  #
  # = Basic usage
  #
  #    message = Poseidon::Message.new(:value => "hello", 
  #                                    :key => "user:123", 
  #                                    :topic => "salutations")
  #
  # = Sending a message
  #
  # When sending a message you must set the topic for the message, this
  # can be done during creation or afterwards.
  #
  # = Compression
  #
  # In normal usage you should never have to worry about compressed
  # Message objects. When producing the producer takes care of 
  # compressing the messages and when fetching the fetcher will 
  # return them decompressed.
  #
  # @api private
  class Message
    # Last 3 bits are used to indicate compression
    COMPRESSION_MASK = 0x7
    MAGIC_TYPE = 0

    # Build a new Message object from its binary representation
    #
    # @param [ResponseBuffer] buffer
    #   a response buffer containing binary data representing a message.
    #
    # @return [Message]
    def self.read(buffer)
      m = Message.new
      m.struct = Protocol::MessageWithOffsetStruct.read(buffer)
      if m.struct.message.checksum_failed?
        raise Errors::ChecksumError
      end
      m
    end

    attr_accessor :struct, :topic

    # Create a new message object
    # 
    # @param [Hash] options
    #
    # @option options [String] :value (nil)
    #   The messages value. Optional.
    #
    # @option options [String] :key (nil)
    #   The messages key. Optional.
    #
    # @option options [String] :topic (nil)
    #   The topic we should send this message to. Optional.
    #
    # @option options [String] :attributes (nil)
    #   Attributes field for the message currently only idicates
    #   whether or not the message is compressed.
    def initialize(options = {})
      build_struct(options)

      @topic = options.delete(:topic)

      if options.any?
        raise ArgumentError, "Unknown options: #{options.keys.inspect}"
      end
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

    # Write a binary representation of the message to buffer
    #
    # @param [RequestBuffer] buffer
    # @return [nil]
    def write(buffer)
      @struct.write(buffer)
      nil
    end

    # @return [String] the Message's key
    def key
      @struct.message.key
    end

    # @return [String] the Message's value
    def value
      @struct.message.value
    end

    # @return [Integer] the Message's offset
    def offset
      @struct.offset
    end

    # Is the value compressed?
    #
    # @return [Boolean] 
    def compressed?
      compression_codec_id > 0
    end

    # Decompressed value
    #
    # @return [String] decompressed value
    def decompressed_value
      compression_codec.decompress(value)
    end

    private
    def attributes
      @struct.message.attributes
    end

    def compression_codec
      Compression.find_codec(compression_codec_id)
    end

    def compression_codec_id
      attributes & COMPRESSION_MASK
    end

    def build_struct(options)
      message_struct = Protocol::MessageStruct.new(
        MAGIC_TYPE,
        options.delete(:attributes) || 0,
        options.delete(:key),
        options.delete(:value)
      )
      struct = Protocol::MessageWithOffsetStruct.new(options.delete(:offset) || 0, message_struct)
      self.struct = struct
    end
  end
end
