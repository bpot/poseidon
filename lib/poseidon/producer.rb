module Poseidon
  # Provides a high level interface for sending messages to a cluster
  # of Kafka brokers.
  #
  # ## Producer Creation
  #
  # Producer requires a broker list and a client_id:
  #
  #     producer = Producer.new(["broker1:port1", "broker2:port1"], "my_client_id",
  #                             :type => :sync)
  #
  # The broker list is only used to bootstrap our knowledge of the cluster --
  # it does not need to contain every broker. The client id should be unique
  # across all clients in the cluster.
  #
  # ## Sending Messages
  #
  # Messages must have a topic before being sent:
  #
  #     messages = []
  #     messages << Poseidon::MessageToSend.new("topic1", "Hello Word")
  #     messages << Poseidon::MessageToSend.new("user_updates_topic", user.update, user.id)
  #     producer.send_messages(messages)
  #
  # ## Producer Types
  #
  # There are two types of producers: sync and async. They can be specified
  # via the :type option when creating a producer.
  #
  # ## Sync Producer
  #
  # The :sync producer blocks while sends messages to the cluster. The more
  # messages you can send per #send_messages call the more efficient it will
  # be.
  #
  # ## Compression
  #
  # When creating the producer you can specify a compression method:
  #
  #     producer = Producer.new(["broker1:port1"], "my_client_id",
  #                             :type => :sync, :compression_codec => :gzip)
  #
  # If you don't specify which topics to compress it will compress all topics.
  # You can specify a set of topics to compress when creating the producer:
  #
  #     producer = Producer.new(["broker1:port1"], "my_client_id",
  #                             :type => :sync, :compression_codec => :gzip,
  #                             :compressed_topics => ["compressed_topic_1"])
  #
  # ## Partitioning
  #
  # For keyless messages the producer will round-robin messages to all 
  # _available_ partitions for at topic. This means that if we are unable to
  # send messages to a specific broker we'll retry sending those to a different
  # broker.
  #
  # However, if you specify a key when creating the message, the producer
  # will choose a partition based on the key and only send to that partition.
  # 
  # ## Custom Partitioning
  #
  # You may also specify a custom partitioning scheme for messages by passing
  # a Proc (or any object that responds to #call) to the Producer.  The proc
  # must return a Fixnum >= 0 and less-than partition_count.
  #
  #     my_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
  #
  #     producer = Producer.new(["broker1:port1", "broker2:port1"], "my_client_id",
  #                             :type => :sync, :partitioner => my_partitioner)
  #
  # @api public
  class Producer
    # @api private
    VALID_OPTIONS = [
      :type,
      :compression_codec,
      :compressed_topics,
      :metadata_refresh_interval_ms,
      :partitioner,
      :max_send_retries,
      :retry_backoff_ms,
      :required_acks,
      :ack_timeout_ms
    ]

    # @api private
    OPTION_DEFAULTS = {
      :type => :sync
    }

    # Returns a new Producer.
    #
    # @param [Array<String>] brokers An array of brokers in the form "host1:port1"
    #
    # @param [String] client_id A client_id used to indentify the producer.
    #
    # @param [Hash] options
    #
    # @option options [:sync / :async] :type (:sync)
    #   Whether we should send messages right away or queue them and send
    #   them in the background.
    #
    # @option options [:gzip / :snappy / :none] :compression_codec (:none)
    #   Type of compression to use.
    # 
    # @option options [Enumberable<String>] :compressed_topics (nil)
    #   Topics to compress.  If this is not specified we will compress all
    #   topics provided that +:compression_codec+ is set.
    #
    # @option options [Integer: Seconds] :metadata_refresh_interval_ms (600_000)
    #   How frequently we should update the topic metadata in milliseconds.
    #
    # @option options [#call, nil] :partitioner
    #   Object which partitions messages based on key. 
    #   Responds to #call(key, partition_count).
    #
    # @option options [Integer] :max_send_retries (3)
    #   Number of times to retry sending of messages to a leader.
    #
    # @option options [Integer] :retry_backoff_ms (100)
    #   The amount of time (in milliseconds) to wait before refreshing the metadata
    #   after we are unable to send messages. 
    #   Number of times to retry sending of messages to a leader.
    #
    # @option options [Integer] :required_acks (0)
    #   The number of acks required per request.
    #
    # @option options [Integer] :request_timeout_ms (1500)
    #   How long the producer waits for acks.
    #
    # @api public
    def initialize(brokers, client_id, options = {})
      options = options.dup
      validate_options(options)

      if !brokers.respond_to?(:each)
        raise ArgumentError, "brokers must respond to #each"
      end
      @brokers    = brokers
      @client_id  = client_id
      @producer   = build_producer(options)
      @shutdown   = false
    end

    # Send messages to the cluster.
    #
    # @param [Enumerable<MessageToSend>] messages
    #   Messages must have a +topic+ set and may have a +key+ set.
    #
    # @return [Boolean]
    #
    # @api public
    def send_messages(messages)
      raise Errors::ProducerShutdownError if @shutdown
      if !messages.respond_to?(:each)
        raise ArgumentError, "messages must respond to #each"
      end

      @producer.send_messages(convert_to_messages_objects(messages))
    end

    # Closes all open connections to brokers
    def shutdown
      @shutdown = true
      @producer.shutdown
    end

    private
    def validate_options(options)
      unknown_keys = options.keys - VALID_OPTIONS
      if unknown_keys.any?
        raise ArgumentError, "Unknown options: #{unknown_keys.inspect}"
      end

      @type = options.delete(:type) || :sync
    end

    def convert_to_messages_objects(messages)
      messages.map do |m|
        Message.new(:value => m.value, :topic => m.topic, :key => m.key)
      end
    end

    def build_producer(options)
      case @type
      when :sync
        SyncProducer.new(@client_id, @brokers, options)
      when :async
        raise "Not implemented yet"
      end
    end
  end
end
