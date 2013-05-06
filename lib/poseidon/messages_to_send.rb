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
    def initialize(messages, cluster_metadata)
      @messages         = messages
      @cluster_metadata = cluster_metadata

      build_topic_set
    end

    def needs_metadata?
      !@cluster_metadata.have_metadata_for_topics?(topic_set)
    end

    def messages_for_brokers(message_conductor)
      topic_metadatas = @cluster_metadata.metadata_for_topics(topic_set)
      MessagesToSendBatch.new(@messages, message_conductor).messages_for_brokers
    end

    def successfully_sent(messages_for_broker)
      @messages -= messages_for_broker.messages
    end

    def all_sent?
      !@messages.any?
    end

    private
    def build_topic_set
      @topic_set = Set.new
      @messages.each { |m| @topic_set.add(m.topic) }
    end
  end
end
