module Poseidon
  # Messages that should be sent to a particular broker.
  # @api private
  class MessagesForBroker
    attr_reader :broker_id, :messages

    def initialize(broker_id)
      @broker_id = broker_id
      @topics = {}
      @messages = []
    end

    # Add a messages for this broker
    def add(message, partition_id)
      @messages << message

      @topics[message.topic] ||= {}
      @topics[message.topic][partition_id] ||= []
      @topics[message.topic][partition_id] << message
    end

    # Build protocol objects for this broker!
    def build_protocol_objects(compression_config)
      @topics.map do |topic, messages_by_partition|
        codec = compression_config.compression_codec_for_topic(topic)

        messages_for_partitions = messages_by_partition.map do |partition, messages|
          message_set = MessageSet.new(messages)
          if codec
            Protocol::MessagesForPartition.new(partition, message_set.compress(codec))
          else
            Protocol::MessagesForPartition.new(partition, message_set)
          end
        end
        Protocol::MessagesForTopic.new(topic, messages_for_partitions)
      end
    end
  end
end
