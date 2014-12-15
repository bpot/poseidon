module Poseidon
  class RecordBatch
    attr_reader :topic_partition
    def initialize(topic_partition)
      @topic_partition = topic_partition
      @produce_request_result = ProduceRequestResult.new
      # XXX
      @memory_records = MemoryRecords.new(nil, nil)
      @thunks = []
      @attempts = 0
      @record_count = 0
    end

    def topic
      @topic_partition.topic
    end

    def partition
      @topic_partition.partition
    end

    def done(base_offset, error = nil)
      @produce_request_result.done(@topic_partition, base_offset, error)
      @thunks.each do |callback, future|
        if error.nil?
          callback.call(future, nil)
        else
          callback.call(nil, error)
        end
      end
    end

    def try_append(key, value, callback = nil)
      @memory_records.append(key, value)
      future = FutureRecordMetadata.new(@produce_request_result, @record_count)
      if callback
        @thunks << [callback, future]
      end
      @record_count += 1
      future
    end

    def message_set
      @memory_records.to_s
    end
  end
end
