module Poseidon
  class MemoryRecords
    include Protocol

    def initialize(compression, size_limit)
      @compression = compression
      @size_limit = size_limit
      @request_buffer = Protocol::RequestBuffer.new
    end

    def append(key, value)
      struct = MessageWithOffsetStruct.new(0,
                                            MessageStruct.new(
                                             0,
                                             0,
                                             key,
                                             value
      ))
      struct.write(@request_buffer)
    end

    def to_s
      message_set = @request_buffer.to_s
      [message_set.bytesize].pack("N") + message_set
    end
  end
end
