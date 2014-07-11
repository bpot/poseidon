module Poseidon
  # A set of messages that we need to send to the cluster. May be used
  # across multiple send attempts.
  #
  # If a custom partitioner is not used than a messages are distributed
  # in round robin fasion to each partition with an available leader.
  #
  # @api private
  class MessagesToSend
    class InvalidPartitionError < StandardError; end
    attr_reader :topic_set, :messages

    # Create a new messages to send object.
    #
    # @param [Array<Message>] messages List of messages we want to send.
    # @param [ClusterMetadta] cluster_metadata
    def initialize(messages, cluster_metadata, callback)
      @messages         = messages
      @cluster_metadata = cluster_metadata
      @callback         = callback
      build_topic_set
    end

    def needs_metadata?
      !@cluster_metadata.have_metadata_for_topics?(topic_set)
    end

    def pending_messages?
      @messages.any?
    end

    def messages_for_brokers(message_conductor)
      topic_metadatas = @cluster_metadata.metadata_for_topics(topic_set)
      MessagesToSendBatch.new(@messages, message_conductor).messages_for_brokers
    end

    def handle_failure_to_fetch_metadata(error)
    end

    # @param [MessagesForBroker] Provided by the messages_for_brokers call
    # @param [ProduceResponse] If we received a response include.
    def handle_produce_response(messages_for_broker, response = nil)
    end

    # @param [MessagesForBroker] Provided by the messages_for_brokers call
    # @param [Exception] Error received when trying to send produce response
    def handle_produce_error(messages_for_broker, error)
    end

=begin
    def produce_response(messages_sent)
      @messages -= messages_sent
      build_topic_set
      nil
    end

    def remove_for_topic(topic)
      for_topic = @messages.select { |m| m.topic == topic }
      @messages -= for_topic
      @topic_set.delete(topic)
      for_topic
    end

=end
    private
    def build_topic_set
      @topic_set = Set.new
      @messages.each { |m| @topic_set.add(m.topic) }
    end
  end
end
