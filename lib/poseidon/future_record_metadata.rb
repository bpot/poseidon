module Poseidon
  class FutureRecordMetadata
    def initialize(produce_request_result, record_offset)
      @producer_request_result = produce_request_result
      @record_offset = record_offset
    end
    
    def wait
      @producer_request_result.wait
    end

    def get
      @producer_request_result.wait
      value_or_error
    end

    private
    def value_or_error
      @value ||= RecordMetadata.new(@producer_request_result.topic_partition, @producer_request_result.base_offset, @record_offset)
    end
  end
end
