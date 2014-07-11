module Poseidon
  # Represents the result of a produce attempt against a kafka topic.
  class ProduceResult
    def self.metadata_failure(topic, messages)
      result = new
      result.messages = messages
      result.topic = topic
      result.error = Poseidon::Errors::UnableToFetchMetadata
      result
    end

    attr_accessor :messages, :topic, :error
    def initialize
      #@produce_topic_response = produce_topic_response
      #@produce_partition_response = produce_partition_response
      @messages = messages
      @error = nil
    end

    # Was the produce request successful?
    #
    # NOTE: This will return false if required_acks is > 1 and the leader
    # timedout waiting for a response from the replicas. In this case
    # trying to resend the messages will likely lead to duplicate messages
    # because the leader will have succesfully persisted the message.
    #
    # You can use the `timeout?` method to differentiate between this case
    # and other failures.
    #
    # @return [Boolean]
    def success?
      !@error
    #  @produce_partition_response.error == Poseidon::Errors::NO_ERROR_CODE
    end

    # Did we fail to receive the required number of acks?
    #
    # @return [Boolean]
    def timeout?
      @produce_partition_response.error_class == Poseidon::Errors::RequestTimedOut
    end

=begin
    # Return an error if we recieved one.
    #
    # @return [Poseidon::Errors::ProtocolError,Nil]
    def error
      if !success?
        @produce_partition_response.error_class
      else
        nil
      end
    end
=end

=begin
    # The messages we the produce request sent.
    #
    # @return [Array<MessageToSend>]
    def messages
      @messages
    end

    # The topic we sent the messages to.
    #
    # @return [String]
    def topic

      #@produce_topic_response.topic
    end
=end

    # The partition we sent the message to.
    #
    # @return [Fixnum]
    def partition
      @produce_partition_response.partition
    end

    # The offset of the first message the broker persisted from this batch.
    #
    # @return [Fixnum]
    def offset
      @produce_partition_response.offset
    end
  end
end
