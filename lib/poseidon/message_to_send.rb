module Poseidon
  # A message we want to send to Kafka. Comprised of the
  # topic we want to send it to, the body of the message
  # and an optional key.
  #
  #     mts = Poseidon::MessageToSend.new("topic", "value", "opt_key")
  #
  # @api public
  class MessageToSend
    attr_reader :value, :key, :topic, :partition

    # Create a new message for sending to a Kafka broker.
    #
    # @param [String] topic
    #   Topic this message should be sent to.
    #
    # @param [String] value
    #   Value of the message we want to send. 
    #
    # @param [String] key
    #   Optional. Message's key, used to route a message
    #   to a specific broker.  Otherwise, messages will be
    #   sent to brokers in a round-robin manner.
    #
    # @api public
    def initialize(topic, value, key = nil)
      raise ArgumentError, "Must provide a non-nil topic" if topic.nil?
      @topic  = topic
      @value  = value
      @key    = key
      @partition = partition
    end
  end
end
