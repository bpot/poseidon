module Poseidon
  # Represents the result of a produce attempt against a kafka topic.
  class ProduceResult
    def initialize(produce_topic_response, produce_partition_response, messages)
      @produce_topic_response = produce_topic_response
      @produce_partition_response = produce_partition_response
      @messages = messages
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
      produce_partition_response.error == Poseidon::NO_ERROR_CODE
    end

    # Did we fail to receive the required number of acks?
    #
    # @return [Boolean]
    def timeout?
      produce_partition_response.error == Poseidon::Error::RequestTimedOut
    end

    # Return an error if we recieved one.
    #
    # @return [Poseidon::Error::ProtocolError,Nil]
    def error
      if !success?
        produce_partition_response.error_class
      else
        nil
      end
    end

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
      @produce_topic_response.topic
    end

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
