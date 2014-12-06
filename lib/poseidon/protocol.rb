module Poseidon
  # @api private
  # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
  module Protocol
    require "poseidon/protocol/protocol_struct"
    require "poseidon/protocol/request_buffer"
    require "poseidon/protocol/response_buffer"

    API_KEYS = {
      :produce => 0,
      :fetch => 1,
      :offset => 2,
      :metadata => 3,
      # 4-7 Non-user facing control APIs
      :offset_commit => 8,
      :offset_fetch => 9,
      :consumer_metadata => 10
    }

    # Request/Response Common Structures
    RequestCommon = ProtocolStruct.new(:api_key => :int16,
                                       :api_version => :int16,
                                       :correlation_id => :int32,
                                       :client_id => :string)
    ResponseCommon = ProtocolStruct.new(:correlation_id => :int32)

    # MessageSet Common Structure
    MessageStruct = ProtocolStruct.new(:magic_type => :int8,
                                       :attributes => :int8,
                                       :key => :bytes,
                                       :value => :bytes).prepend_size.prepend_crc32.truncatable
    MessageWithOffsetStruct = ProtocolStruct.new(:offset => :int64,
                                                 :message => MessageStruct)
     
    # When part of produce requests of fetch responses a MessageSet
    # has a prepended size.  When a MessageSet is compressed and
    # nested in a Message size is not prepended.
    MessageSetStruct = ProtocolStruct.new(:messages => [Message]).
                                          size_bound_array(:messages)
    MessageSetStructWithSize = MessageSetStruct.dup.prepend_size

    # Produce Request
    MessagesForPartition = ProtocolStruct.new(:partition    => :int32,
                                              :message_set  => MessageSet)
    MessagesForTopic = ProtocolStruct.new(:topic => :string,
                                          :messages_for_partitions =>
                                            [MessagesForPartition])
    ProduceRequest = ProtocolStruct.new(:common => RequestCommon,
                                        :required_acks => :int16,
                                        :timeout => :int32,
                                        :messages_for_topics => [MessagesForTopic])

    # Produce Response
    ProducePartitionResponse = ProtocolStruct.new(:partition => :int32,
                                                  :error => :int16,
                                                  :offset => :int64)
    ProduceTopicResponse = ProtocolStruct.new(:topic => :string,
                                              :partitions => [ProducePartitionResponse])
    ProduceResponse = ProtocolStruct.new(:common => ResponseCommon,
                                         :topic_response => [ProduceTopicResponse])

    # Fetch Request
    PartitionFetch = ProtocolStruct.new(:partition => :int32,
                                        :fetch_offset => :int64,
                                        :max_bytes => :int32)
    TopicFetch = ProtocolStruct.new(:topic => :string,
                                    :partition_fetches => [PartitionFetch])
    FetchRequest = ProtocolStruct.new(:common => RequestCommon,
                                      :replica_id => :int32,
                                      :max_wait_time => :int32,
                                      :min_bytes => :int32,
                                      :topic_fetches => [TopicFetch])

    # Fetch Response
    PartitionFetchResponse = ProtocolStruct.new(:partition => :int32,
                                                :error => :int16,
                                                :highwater_mark_offset => :int64,
                                                :message_set => MessageSet)
    TopicFetchResponse = ProtocolStruct.new(:topic => :string,
                                            :partition_fetch_responses => [PartitionFetchResponse])
    FetchResponse = ProtocolStruct.new(
      :common => ResponseCommon,
      :topic_fetch_responses => [TopicFetchResponse])

    # Offset Request
    PartitionOffsetRequest = ProtocolStruct.new(:partition => :int32,
                                                :time => :int64,
                                                :max_number_of_offsets => :int32)
    TopicOffsetRequest = ProtocolStruct.new(
      :topic => :string,
      :partition_offset_requests => [PartitionOffsetRequest])
    OffsetRequest = ProtocolStruct.new(:common => RequestCommon,
                                       :replica_id => :int32,
                                       :topic_offset_requests => [TopicOffsetRequest])

    # Offset Response
    Offset = ProtocolStruct.new(:offset => :int64)
    PartitionOffset = ProtocolStruct.new(:partition => :int32,
                                         :error => :int16,
                                         :offsets => [Offset])
    TopicOffsetResponse = ProtocolStruct.new(:topic => :string,
                                             :partition_offsets => [PartitionOffset])
    OffsetResponse = ProtocolStruct.new(
      :common => ResponseCommon,
      :topic_offset_responses => [TopicOffsetResponse])

    # Metadata Request
    MetadataRequest = ProtocolStruct.new( :common => RequestCommon,
                                          :topic_names => [:string])

    # Metadata Response
    Broker = ProtocolStruct.new(:id => :int32,
                                :host => :string,
                                :port => :int32)
    PartitionMetadata = ProtocolStruct.new(:error     => :int16,
                                           :id        => :int32,
                                           :leader    => :int32,
                                           :replicas  => [:int32],
                                           :isr       => [:int32])
    TopicMetadataStruct = ProtocolStruct.new(:error => :int16,
                                       :name => :string,
                                       :partitions => [PartitionMetadata])
    MetadataResponse = ProtocolStruct.new(:common => ResponseCommon,
                                          :brokers => [Broker],
                                          :topics => [TopicMetadata])

    # Consumer Metadata Request
    ConsumerMetadataRequest = ProtocolStruct.new(:consumer_group => :string)

    # Consumer Metadata Response
    ConsumerMetadataResponse = ProtocolStruct.new(:error => :int16,
                                                  :coordinator_id => :int32,
                                                  :coordinator_host => :string,
                                                  :coordinator_port => :int32)

    # Offset Commit Request
    OffsetCommitRequest = ProtocolStruct.new(:consumer_group => :string,
                                             :topic_name => :string,
                                             :partition => :int32,
                                             :offset => :int64,
                                             :template_stamp => :int64,
                                             :metadata => :string)

    # Offset Commit Response
    OffsetCommitPartitionResponse = ProtocolStruct.new(:partition => :int32,
                                                       :error_code => :int16)
    OffsetCommitTopicResponse = ProtocolStruct.new(:topic_name => :string,
                                                   :partitions => [OffsetCommitPartitionResponse])
    OffsetCommitResponse = ProtocolStruct.new(:topic_responses => [OffsetCommitTopicResponse])

    # Offset Fetch Request
    OffsetFetchPartitionRequest = ProtocolStruct.new(:partition => :int32)
    OffsetFetchTopicRequest = ProtocolStruct.new(:topic_name => :string,
                                                 :partitions => [OffsetFetchPartitionRequest])
    OffsetFetchRequest = ProtocolStruct.new(:consumer_group => :string,
                                            :topic_requests => [OffsetFetchTopicRequest])

    # Offset Fetch Response
    OffsetFetchPartitionResponse = ProtocolStruct.new(:partition => :int32,
                                                      :offset => :int64,
                                                      :metadata => :string,
                                                      :error_code => :int16)
    OffsetFetchTopicResponse = ProtocolStruct.new(:topic_name => :string,
                                                  :partitions => [OffsetFetchPartitionResponse])
    OffsetFetchResponse = ProtocolStruct.new(:topic_responses => [OffsetFetchTopicResponse])
  end
end
