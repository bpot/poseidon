module Poseidon
  # Encapsulates what we known about brokers, topics and partitions
  # from Metadata API calls.
  #
  # @api private
  class ClusterMetadata
    attr_reader :brokers, :last_refreshed_at, :topic_metadata
    def initialize
      @brokers        = {}
      @topic_metadata = {}
      @last_refreshed_at = nil
    end

    # Update what we know about the cluter based on MetadataResponse
    # 
    # @param [MetadataResponse] topic_metadata_response
    # @return nil
    def update(topic_metadata_response)
      update_brokers(topic_metadata_response.brokers)
      update_topics(topic_metadata_response.topics)

      @last_refreshed_at = Time.now
      nil
    end

    # Do we have metadata for these topics already?
    #
    # @param [Enumberable<String>] topic_names A set of topics.
    # @return [Boolean] true if we have metadata for all +topic_names+, otherwise false.
    def have_metadata_for_topics?(topic_names)
      topic_names.all? { |topic| @topic_metadata[topic] }
    end

    # Provides metadata for each topic
    #
    # @param [Enumerable<String>] topic_names Topics we should return metadata for
    # @return [Hash<String,TopicMetadata>]
    def metadata_for_topics(topic_names)
      Hash[topic_names.map { |name| [name, @topic_metadata[name]] }]
    end

    # Provides a Broker object for +broker_id+. This corresponds to the
    # broker ids in the TopicMetadata objects.
    #
    # @param [Integer] broker_id Broker id 
    def broker(broker_id)
      @brokers[broker_id]
    end

    # Return lead broker for topic and partition
    def lead_broker_for_partition(topic_name, partition)
      broker_id = @topic_metadata[topic_name].partition_leader(partition)
      if broker_id
        @brokers[broker_id]
      else
        nil
      end
    end

    private
    def update_topics(topics)
      topics.each do |topic|
        @topic_metadata[topic.name] = topic
      end
    end

    def update_brokers(brokers)
      brokers.each do |broker|
        @brokers[broker.id] = broker
      end
    end
  end
end
