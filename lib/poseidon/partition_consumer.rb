module Poseidon
  # A primitive Kafka Consumer which operates on a specific broker, topic and partition.
  #
  # Example in the README.
  #
  # @api public
  class PartitionConsumer
    # The offset of the latest message the broker recieved for this partition.
    # Useful for knowning how far behind the consumer is. This value is only
    # as recent as the last fetch call.
    attr_reader :highwater_mark

    attr_reader :host, :port

    attr_reader :offset

    attr_reader :topic

    # Returns a consumer pointing at the lead broker for the partition.
    #
    # Eventually this will be replaced by higher level consumer functionality,
    # this is a stop-gap.
    #
    def self.consumer_for_partition(client_id, seed_brokers, topic, partition, offset, options = {})

      broker = BrokerPool.open(client_id, seed_brokers, options[:socket_timeout_ms] || 10_000) do |broker_pool|
        cluster_metadata = ClusterMetadata.new
        cluster_metadata.update(broker_pool.fetch_metadata([topic]))

        cluster_metadata.lead_broker_for_partition(topic, partition)
      end

      new(client_id, broker.host, broker.port, topic, partition, offset, options)
    end

    # Create a new consumer which reads the specified topic and partition from
    # the host.
    #
    # @param [String] client_id  Used to identify this client should be unique.
    # @param [String] host
    # @param [Integer] port 
    # @param [String] topic Topic to read from
    # @param [Integer] partition Partitions are zero indexed.
    # @param [Integer,Symbol] offset 
    #   Offset to start reading from. A negative offset can also be passed.
    #   There are a couple special offsets which can be passed as symbols:
    #     :earliest_offset       Start reading from the first offset the server has.
    #     :latest_offset         Start reading from the latest offset the server has.
    #
    # @param [Hash] options
    #   Theses options can all be overridden in each individual fetch command.
    #
    # @option options [:max_bytes] Maximum number of bytes to fetch
    #   Default: 1048576 (1MB)
    #
    # @option options [:max_wait_ms] 
    #   How long to block until the server sends us data.
    #   NOTE: This is only enforced if min_bytes is > 0.
    #   Default: 100 (100ms)
    #
    # @option options [:min_bytes] Smallest amount of data the server should send us.
    #   Default: 1 (Send us data as soon as it is ready)
    #
    # @option options [:socket_timeout_ms]
    #   How long to wait for reply from server. Should be higher than max_wait_ms.
    #   Default: 10000 (10s)
    #
    # @api public
    def initialize(client_id, host, port, topic, partition, offset, options = {})
      @host = host
      @port = port

      handle_options(options)

      @connection = Connection.new(host, port, client_id, @socket_timeout_ms)
      @topic = topic
      @partition = partition
      if Symbol === offset
        raise ArgumentError, "Unknown special offset type: #{offset}" unless [:earliest_offset, :latest_offset].include?(offset)
      end
      @offset = offset
    end

    # Fetch messages from the broker.
    #
    # @param [Hash] options
    #
    # @option options [:max_bytes]
    #   Maximum number of bytes to fetch
    #
    # @option options [:max_wait_ms]
    #   How long to block until the server sends us data.
    #
    # @option options [:min_bytes] 
    #   Smallest amount of data the server should send us.
    #
    # @api public
    def fetch(options = {})
      fetch_max_wait = options.delete(:max_wait_ms) || max_wait_ms
      fetch_max_bytes = options.delete(:max_bytes) || max_bytes
      fetch_min_bytes = options.delete(:min_bytes) || min_bytes

      if options.keys.any?
        raise ArgumentError, "Unknown options: #{options.keys.inspect}"
      end

      topic_fetches = build_topic_fetch_request(fetch_max_bytes)
      fetch_response = @connection.fetch(fetch_max_wait, fetch_min_bytes, topic_fetches)
      topic_response = fetch_response.topic_fetch_responses.first 
      partition_response = topic_response.partition_fetch_responses.first

      unless partition_response.error == Errors::NO_ERROR_CODE
        if @offset < 0 &&
          Errors::ERROR_CODES[partition_response.error] == Errors::OffsetOutOfRange
          @offset = :earliest_offset
          return fetch(options)
        end

        raise Errors::ERROR_CODES[partition_response.error]
      else
        @highwater_mark = partition_response.highwater_mark_offset
        messages = partition_response.message_set.flatten.map do |m|
          FetchedMessage.new(topic_response.topic, m.value, m.key, m.offset)
        end
        if messages.any?
          @offset = messages.last.offset + 1
        end
        messages
      end
    end

    # @return [Integer] next offset we will fetch
    #
    # @api public
    def next_offset
      resolve_offset_if_necessary
      @offset
    end

    # Close the connection to the kafka broker
    #
    # @return [Nil]
    #
    # @api public
    def close
      @connection.close
      nil
    end

    private
    def handle_options(options)
      @max_bytes         = options.delete(:max_bytes) || 1024*1024
      @min_bytes         = options.delete(:min_bytes) || 1
      @max_wait_ms       = options.delete(:max_wait_ms) || 10_000
      @socket_timeout_ms = options.delete(:socket_timeout_ms) || @max_wait_ms + 10_000

      if @socket_timeout_ms < @max_wait_ms
        raise ArgumentError, "Setting socket_timeout_ms should be higher than max_wait_ms"
      end

      if options.keys.any?
        raise ArgumentError, "Unknown options: #{options.keys.inspect}"
      end
    end

    def max_wait_ms
      @max_wait_ms
    end

    def max_bytes
      @max_bytes
    end

    def min_bytes
      @min_bytes
    end

    def resolve_offset_if_necessary
      return unless Symbol === @offset || @offset < 0

      protocol_offset = case @offset
      when :earliest_offset
        -2
      when :latest_offset
        -1
      else
        -1
      end

      topic_offset_responses = @connection.offset(build_topic_offset_request(protocol_offset))
      partition_offsets = topic_offset_responses.first.partition_offsets
      if partition_offsets.first.error != Errors::NO_ERROR_CODE
        raise Errors::ERROR_CODES[partition_offsets.first.error]
      end

      offset_struct = partition_offsets.first.offsets.first

      @offset = if offset_struct.nil?
        0
      elsif @offset.kind_of?(Fixnum) && @offset < 0
        offset_struct.offset + @offset
      else
        offset_struct.offset
      end
    end

    def build_topic_offset_request(protocol_offset)
      partition_offset_request = Protocol::PartitionOffsetRequest.new(
        @partition,
        protocol_offset,
        max_number_of_offsets = 1)
        
      [Protocol::TopicOffsetRequest.new(topic, [partition_offset_request])]
    end

    def build_topic_fetch_request(max_bytes)
      partition_fetches = [Protocol::PartitionFetch.new(@partition,
                                                        next_offset,
                                                        max_bytes)]
      topic_fetches = [Protocol::TopicFetch.new(topic, partition_fetches)]
    end
  end
end
