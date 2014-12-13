module Poseidon
  class RecordBatch
    attr_reader :records
    def initialize(topic_partition)
      @topic_partition = topic_partition
      @records = []
      @produce_request_result = Concurrent::IVar.new
    end

    def topic
      @topic_partition.topic
    end

    def partition
      @topic_partition.partition
    end

    def wait
      @producer_request_result.wait
    end

    def get
      wait
      raise "Huh"
    end

    def try_append(key, value, callback = nil)
      @records << {
        key: key,
        value: value
      }
      FutureRecordMetadata.new(@produce_request_result, @records.size - 1)
    end
  end
end
