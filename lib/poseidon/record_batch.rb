module Poseidon
  class RecordBatch
    attr_reader :records, :topic_partition
    def initialize(topic_partition)
      @topic_partition = topic_partition
      @produce_request_result = ProduceRequestResult.new
      @records = []
      @thunks = []
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
          callback.call(future)
        else
          raise "ZOMG DONNU BOUT ERRRS"
        end
      end
    end

    def try_append(key, value, callback = nil)
      @records << {
        key: key,
        value: value
      }
      future = FutureRecordMetadata.new(@produce_request_result, @records.size - 1)
      if callback
        @thunks << [callback, future]
      end
      future
    end
  end
end
