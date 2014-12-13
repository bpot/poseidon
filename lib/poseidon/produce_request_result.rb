module Poseidon
  class ProduceRequestResult
    attr_reader :topic_partition, :base_offset, :error

    def initialize
      @ivar = Concurrent::IVar.new
    end

    def wait
      @ivar.wait
    end

    def done(topic_partition, base_offset, error = nil)
      @topic_partition = topic_partition
      @base_offset = base_offset
      @error = error
      @ivar.set(true)
    end
  end
end
