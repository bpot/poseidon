module Poseidon
  # Used by +Producer+ for sending messages to the kafka cluster.
  #
  # You should not use this interface directly
  #
  # Fetches metadata at appropriate times.
  # Builds MessagesToSend
  # Handle MessageBatchToSend lifecyle
  #
  # Who is responsible for fetching metadata from broker seed list?
  #   Do we want to be fetching from real live brokers eventually?
  #
  # @api private
  class SyncProducer
    OPTION_DEFAULTS = {
      :compression_codec => nil,
      :compressed_topics => nil,
      :metadata_refresh_interval_ms => 600_000,
      :partitioner => nil,
      :max_send_retries => 3,
      :retry_backoff_ms => 100,
      :required_acks => 0,
      :ack_timeout_ms => 1500,
      :socket_timeout_ms => 10_000
    }

    attr_reader :client_id, :retry_backoff_ms, :max_send_retries,
      :metadata_refresh_interval_ms, :required_acks, :ack_timeout_ms, :socket_timeout_ms
    def initialize(client_id, seed_brokers, options = {})
      @client_id = client_id

      handle_options(options.dup)

      @cluster_metadata   = ClusterMetadata.new
      @message_conductor  = MessageConductor.new(@cluster_metadata, @partitioner)
      @broker_pool        = BrokerPool.new(client_id, seed_brokers, socket_timeout_ms)
    end

    def send_messages(messages)
      return if messages.empty?

      messages_to_send = MessagesToSend.new(messages, @cluster_metadata)

      if refresh_interval_elapsed?
        refresh_metadata(messages_to_send.topic_set)
      end

      ensure_metadata_available_for_topics(messages_to_send)

      (@max_send_retries+1).times do
        messages_to_send.messages_for_brokers(@message_conductor).each do |messages_for_broker|
          if sent = send_to_broker(messages_for_broker)
            messages_to_send.successfully_sent(sent)
          end
        end

        if !messages_to_send.pending_messages? || @max_send_retries == 0
          break
        else
          Kernel.sleep retry_backoff_ms / 1000.0
          refresh_metadata(messages_to_send.topic_set)
        end
      end

      if messages_to_send.pending_messages?
        raise "Failed to send all messages: #{messages_to_send.messages} remaining"
      else
        true
      end
    end

    def close
      @broker_pool.close
    end

    alias_method :shutdown, :close

    private

    def ensure_metadata_available_for_topics(messages_to_send)
      return if !messages_to_send.needs_metadata?

      Poseidon.logger.debug { "Fetching metadata for #{messages_to_send.topic_set}. (Attempt 1)" }
      refresh_metadata(messages_to_send.topic_set)
      return if !messages_to_send.needs_metadata?

      2.times do |n|
        sleep 5

        Poseidon.logger.debug { "Fetching metadata for #{messages_to_send.topic_set}. (Attempt #{n+2})" }
        refresh_metadata(messages_to_send.topic_set)
        return if !messages_to_send.needs_metadata?
      end
      raise Errors::UnableToFetchMetadata
    end

    def handle_options(options)
      @ack_timeout_ms    = handle_option(options, :ack_timeout_ms)
      @socket_timeout_ms = handle_option(options, :socket_timeout_ms)
      @retry_backoff_ms  = handle_option(options, :retry_backoff_ms)

      @metadata_refresh_interval_ms = 
        handle_option(options, :metadata_refresh_interval_ms)

      @required_acks    = handle_option(options, :required_acks)
      @max_send_retries = handle_option(options, :max_send_retries)

      @compression_config = ProducerCompressionConfig.new(
        handle_option(options, :compression_codec), 
        handle_option(options, :compressed_topics))

      @partitioner = handle_option(options, :partitioner)

      raise ArgumentError, "Unknown options: #{options.keys.inspect}" if options.keys.any?
    end

    def handle_option(options, sym)
      options.delete(sym) || OPTION_DEFAULTS[sym]
    end

    def refresh_interval_elapsed?
      @cluster_metadata.last_refreshed_at.nil? ||
        (Time.now - @cluster_metadata.last_refreshed_at) * 1000 > metadata_refresh_interval_ms
    end

    def refresh_metadata(topics)
      topics_to_refresh = topics.dup

      @cluster_metadata.topics.each do |topic|
        topics_to_refresh.add(topic)
      end

      @cluster_metadata.update(@broker_pool.fetch_metadata(topics_to_refresh))
      @broker_pool.update_known_brokers(@cluster_metadata.brokers)
    end

    def send_to_broker(messages_for_broker)
      return false if messages_for_broker.broker_id == -1
      to_send = messages_for_broker.build_protocol_objects(@compression_config)

      Poseidon.logger.debug { "Sending messages to broker #{messages_for_broker.broker_id}" }
      response = @broker_pool.execute_api_call(messages_for_broker.broker_id, :produce,
                                              required_acks, ack_timeout_ms,
                                              to_send)
      if required_acks == 0
        messages_for_broker.messages
      else
        messages_for_broker.successfully_sent(response)
      end
    rescue Connection::ConnectionFailedError
      false
    end
  end
end
