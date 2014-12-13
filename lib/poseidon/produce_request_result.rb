module Poseidon
  class ProduceRequestResult
    def initialize
    end

    def done(topic_partition, base_offset, error = nil)
      @topic_partition = topic_partition
      @base_offset = base_offset
      @error = error
    end
  end
end
