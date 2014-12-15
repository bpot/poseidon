module Poseidon
  class RecordAccumulator
    attr_reader :records
    def initialize(retry_backoff_ms)
      @record_batches_by_topic_partition = {}
      @retry_backoff_ms = retry_backoff_ms
    end

    def append(topic, key, value, partition_id, compression, cb)
      topic_partition = TopicPartition.new(topic, partition_id)
      @record_batches_by_topic_partition[topic_partition] ||= RecordBatch.new(topic_partition)
      future = @record_batches_by_topic_partition[topic_partition].try_append(key,value, cb)
      RecordAppendResult.new(future, nil, nil)
    end

    def ready(cluster_metadata)
      ready_brokers = Set.new
      next_ready_check_delay_ms = LONG_MAX
      unknown_leaders_exist = false

      exhausted = false
      @record_batches_by_topic_partition.keys.each do |topic_partition|
        leader = cluster_metadata.lead_broker_for_partition(topic_partition.topic, topic_partition.partition)
        puts "[#{Poseidon.timestamp_ms}] Leader for topic partition #{leader.inspect}"
        if leader.nil?
          unknown_leaders_exist = true
        elsif !ready_brokers.include?(leader)
          batch = @record_batches_by_topic_partition[topic_partition]

          if batch
            now = Poseidon.timestamp_ms
            backing_off = batch.attempts > 0 && batch.last_attempt_ms + @retry_backoff_ms > now
            waited_time_ms = now - batch.last_attempt_ms
            # XXX 0 should be linger_ms
            time_to_wait_ms = backing_off ? @retry_backoff_ms : 0
            time_left_ms = [time_to_wait_ms - waited_time_ms, 0].max
            # XXX Based on number of things
            full = false
            expired = waited_time_ms >= time_to_wait_ms
            sendable = full || expired || exhausted # XXX || closed

            if sendable && !backing_off
              ready_brokers.add(leader)
            else
              next_ready_check_delay_ms = [time_left_ms, next_ready_check_delay_ms].min
            end
          end
        end
      end

      ReadyCheckResult.new(ready_brokers, next_ready_check_delay_ms, unknown_leaders_exist)
    end

    def reenque(record_batch)
      record_batch.attempts += 1
      record_batch.last_attempt_ms = Poseidon.timestamp_ms
      #record_batches = @record_batches_by_topic_partition[record_batch.topic_partition] ||= []
      @record_batches_by_topic_partition[record_batch.topic_partition] = record_batch
    end

    def drain(cluster_metadata, ready_brokers)
      puts "READY BROKERS: #{ready_brokers.inspect}"
      return if ready_brokers.empty?

      batches = {}
      ready_brokers.each do |broker|
        partitions = cluster_metadata.partitions_for_broker(broker)
        partitions.each do |partition|
          batches[broker.id] ||= []
          topic_partition = TopicPartition.new(partition[:topic], partition[:partition].id)
          if record_batch = @record_batches_by_topic_partition[topic_partition]
            batches[broker.id] << record_batch
            @record_batches_by_topic_partition.delete(topic_partition)
          end
        end if partitions
      end
      batches
    end

    def close
    end
  end
end
