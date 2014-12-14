module Poseidon
  class RecordAccumulator
    attr_reader :records
    def initialize
      @record_batches_by_topic_partition = {}
    end

    def add(topic, key, value, partition_id, compression, cb)
      topic_partition = TopicPartition.new(topic, partition_id)
      @record_batches_by_topic_partition[topic_partition] ||= RecordBatch.new(topic_partition)
      @record_batches_by_topic_partition[topic_partition].try_append(key,value, cb)
    end

    def ready(cluster_metadata)
      ready_brokers = Set.new
      next_ready_check_delay_ms = LONG_MAX
      unknown_leaders_exist = false

      # exhausted = false
      @record_batches_by_topic_partition.keys.each do |topic_partition|
        leader = cluster_metadata.lead_broker_for_partition(topic_partition.topic, topic_partition.partition)
        if leader.nil?
          unknown_leaders_exist = true
        elsif !ready_brokers.include?(leader)
          # TODO just asssume sendable now...
          backing_off = false

          ready_brokers.add(leader)
        end
      end

      ReadyCheckResult.new(ready_brokers, next_ready_check_delay_ms, unknown_leaders_exist)
    end

    def drain(cluster_metadata, ready_brokers)
      return if ready_brokers.empty?

      @batches = {}
      ready_brokers.each do |broker|
        @batches[broker.id] ||= []
        partitions = cluster_metadata.partitions_for_broker(broker)
        partitions.each do |partition|
          topic_partition = TopicPartition.new(partition[:topic], partition[:partition].id)
          if record_batch = @record_batches_by_topic_partition[topic_partition]
            @batches[broker.id] << record_batch
            @record_batches_by_topic_partition.delete(topic_partition)
          end
        end
      end
      @batches
    end

    def close
    end
  end
end
