module Poseidon
  # Encapsulates what we known about brokers, topics and partitions
  # from Metadata API calls.
  #
  # @api private
  class ClusterMetadata
    attr_reader :brokers, :last_refreshed_at, :topic_metadata, :topics_of_interest
    def initialize
      @brokers        = {}
      @topic_metadata = {}
      @partitions_by_broker = {}
      @topics_of_interest = Set.new
      @last_refreshed_at = nil
      @need_update = false
      @version = 0
    end

    def add_seed_brokers(seed_brokers)
      broker_id = -1
      seed_brokers.each do |s|
        host, port = s.split(":")
        @brokers[broker_id] = Protocol::Broker.new(broker_id, host, port)

        broker_id -= 1
      end
    end

    def request_update
      @need_update = true

      @version
    end

    def needs_update?
      @need_update
    end

    def add_topic(topic_of_interest)
      @topics_of_interest.add(topic_of_interest)
    end

    # Update what we know about the cluter based on MetadataResponse
    # 
    # @param [MetadataResponse] topic_metadata_response
    # @return nil
    def update(topic_metadata_response)
      update_brokers(topic_metadata_response.brokers)
      update_topics(topic_metadata_response.topics)
      update_broker_to_partition_map(topic_metadata_response.topics)

      @last_refreshed_at = Time.now
      @need_update = false
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

    def partitions_for_broker(broker)
      @partitions_by_broker[broker.id]
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

    def topics
      @topic_metadata.keys
    end

    def to_s
      out = ""
      @topic_metadata.each do |topic, metadata|
        out << "Topic: #{topic}"
        out << "-------------------------"
        out << metadata.to_s
      end
      out
    end

    private
    def update_topics(topics)
      topics.each do |topic|
        if topic.exists?
          @topic_metadata[topic.name] = topic
        end
      end
    end

    def update_broker_to_partition_map(topics)
      topics.each do |topic|
        topic.struct.partitions.each do |partition|
          if partition.leader != -1
            @partitions_by_broker[partition.leader] ||= []
            @partitions_by_broker[partition.leader] << {:topic => topic.name, :partition => partition}
          end
        end
      end
    end

    def update_brokers(brokers)
      brokers.each do |broker|
        @brokers[broker.id] = broker
      end
    end
  end
end
