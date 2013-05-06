module Poseidon
  # A batch of messages for an individual send attempt to the cluster.
  # @api private
  class MessagesToSendBatch
    def initialize(messages, message_conductor)
      @messages           = messages
      @message_conductor  = message_conductor
    end

    # Groups messages by broker and preps them for transmission.
    #
    # @return [Array<MessagesForBroker>]
    def messages_for_brokers
      messages_for_broker_ids = {}
      @messages.each do |message|
        partition_id, broker_id = @message_conductor.destination(message.topic,
                                                                 message.key)

        # Create a nested hash to group messages by broker_id, topic, partition.
        messages_for_broker_ids[broker_id] ||= MessagesForBroker.new(broker_id)
        messages_for_broker_ids[broker_id].add(message, partition_id)
      end

      messages_for_broker_ids.values
    end
  end
end
