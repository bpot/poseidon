module Poseidon
  # @api private
  class MessageConductor
    NO_PARTITION  = -1
    NO_BROKER     = -1
    # Create a new message conductor
    #
    # @param [Hash<String,TopicMetadata>] topics_metadata
    #   Metadata for all topics this conductor may receive.
    # @param [Object] partitioner
    #   Custom partitioner
    def initialize(cluster_metadata, partitioner)
      @cluster_metadata   = cluster_metadata
      @partitioner        = partitioner
      @partition_counter  = -1 
    end

    # Determines which partition a message should be sent to.
    #
    # @param [String] topic
    #   Topic we are sending this message to
    #
    # @param [Object] key
    #   Key for this message, may be nil
    #
    # @return [Integer,Integer] 
    #   partition_id and broker_id to which this message should be sent
    def destination(topic, key = nil)
      topic_metadata = topic_metadatas[topic]
      if topic_metadata && topic_metadata.leader_available?
        partition_id = determine_partition(topic_metadata, key)
        broker_id    = topic_metadata.partitions[partition_id].leader || NO_BROKER
      else
        partition_id  = NO_PARTITION
        broker_id     = NO_BROKER
      end

      return partition_id, broker_id
    end

    private

    def topic_metadatas
      @cluster_metadata.topic_metadata
    end

    def determine_partition(topic_metadata, key)
      if key
        partition_for_keyed_message(topic_metadata, key)
      else
        partition_for_keyless_message(topic_metadata)
      end
    end

    def partition_for_keyed_message(topic_metadata, key)
      partition_count  = topic_metadata.partition_count
      if @partitioner
        partition_id     = @partitioner.call(key, partition_count)

        if partition_id >= partition_count
          raise Errors::InvalidPartitionError, "partitioner (#{@partitioner.inspect}) requested #{partition_id} while only #{partition_count} partitions exist"
        end
      else
        partition_id = Zlib::crc32(key) % partition_count
      end

      partition_id
    end

    def partition_for_keyless_message(topic_metadata)
      partition_count = topic_metadata.available_partition_count

      if partition_count > 0
        next_partition_counter % partition_count
      else
        NO_PARTITION
      end
    end

    def next_partition_counter
      @partition_counter += 1
    end
  end
end
