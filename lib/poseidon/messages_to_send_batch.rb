module Poseidon
  # A batch of messages for an individual send attempt to the cluster.
  # @api private
  class MessagesToSendBatch
    def initialize(messages, partitioner, topic_metadatas)
      @messages           = messages
      @partitioner        = partitioner
      @topic_metadatas    = topic_metadatas
    end

    # Groups messages by broker and preps them for transmission.
    #
    # @return [Array<MessagesForBroker>]
    def messages_for_brokers
      message_conductor = MessageConductor.new(@topic_metadatas, @partitioner)

      messages_for_broker_ids = {}
      @messages.each do |message|
        topic_metadata = @topic_metadatas[message.topic]
        partition_id, broker_id = message_conductor.destination(message.topic,
                                                                message.key)

        # Create a nested hash to group messages by broker_id, topic, partition.
        messages_for_broker_ids[broker_id] ||= MessagesForBroker.new(broker_id)
        messages_for_broker_ids[broker_id].add(message, partition_id)
      end

      messages_for_broker_ids.values
    end
  end
end
