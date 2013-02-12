module Poseidon

  # A message fetched from a Kafka broker.
  #
  # ```
  # fetched_messages = consumer.fetch
  # fetched_messages.each do |fm|
  #   puts "Topic: #{fm.topic}"
  #   puts "Value #{fm.value}"
  #   puts "Key: #{fm.key}"
  #   puts "Offset: #{fm.offset}"
  # end
  # ```
  #
  # @param [String] topic
  #   Topic this message should be sent to.
  #
  # @param [String] value
  #   Value of the message we want to send. 
  #
  # @param [String] key
  #   Optional. Message's key, used to route a message
  #   to a specific broker.  Otherwise, keys will be
  #   sent to brokers in a round-robin manner.
  #
  # @api public
  class FetchedMessage
    attr_reader :value, :key, :topic, :offset

    def initialize(topic, value, key, offset)
      @topic  = topic
      @value  = value
      @key    = key
      @offset = offset
    end
  end
end
