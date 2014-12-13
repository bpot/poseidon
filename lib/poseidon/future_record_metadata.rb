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
    end
  end
end
