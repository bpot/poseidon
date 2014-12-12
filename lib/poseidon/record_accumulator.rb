module Poseidon
  class RecordAccumulator
    attr_reader :records
    def initialize
      @records = []
    end

    def add(topic, key, value, partition_id)
      @records << {
        topic: topic,
        key: key,
        value: value,
        partition_id: partition_id
      }
    end

    def records_by_broker_id(cluster_metadata)
      collated = {}
      @records.each do |h|
        leader = cluster_metadata.lead_broker_for_partition(h[:topic], h[:partition_id])
        if leader
          collated[leader.id] ||= []
          collated[leader.id] << h
        end
      end
      collated
    end
  end
end
