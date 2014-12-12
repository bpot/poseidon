module Poseidon
  class ProducerSender
    def initialize(client, cluster_metadata, record_accumulator)
      @client = client
      @cluster_metadata = cluster_metadata
      @record_accumulator = record_accumulator
    end

    def wakeup
    end

    def run
      records_to_send = @record_accumulator.records_by_broker_id(@cluster_metadata)

      requests = []
      records_to_send.each do |broker_id, records|
        requests << {
          broker_id: broker_id,
          request: build_request(records)
        }
      end

      responses = @client.poll(requests)
    end

    private
    def build_request(records)
    end
  end
end
