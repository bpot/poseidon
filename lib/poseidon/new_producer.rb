require 'concurrent/ivar'
module Poseidon
  class NewProducer
    def initialize(client_id, seed_brokers, options = {})
      @partitioner = nil
      @socket_timeout_ms = 10_0000
      @metadata_fetch_timeout_ms = 60_000
      @metadata_max_age = 5 * 60 * 1000
      @metadata_refresh_interval_ms = 600_000
      @reconnect_backoff_ms = 10
      @refresh_backoff_ms = 100

      @client_id          = client_id
      @cluster_metadata   = ClusterMetadata.new(@refresh_backoff_ms, @metadata_max_age)
      @message_conductor  = MessageConductor.new(@cluster_metadata, @partitioner)

      @cluster_metadata.add_seed_brokers(seed_brokers)

      @selector = Selector.new
      @client = NetworkClient.new(@selector, @client_id, @cluster_metadata, @reconnect_backoff_ms)
      @record_accumulator = RecordAccumulator.new
      @sender = ProducerSender.new(@client, @cluster_metadata, @record_accumulator)
    end

    def send_message(message_to_send, &cb)
      wait_on_metadata(message_to_send.topic)

      #pp @cluster_metadata

      partition_id, _ = @message_conductor.destination(message_to_send.topic, message_to_send.key)

      #puts "Sending to #{partition_id}"
      future = @record_accumulator.add(message_to_send.topic, message_to_send.key, message_to_send.value, partition_id, compression = nil, cb)
      future 
    end

    def close
      @sender.initiate_close
      @sender.join
    end

    private
    def wait_on_metadata(topic)
      return if @cluster_metadata.have_metadata_for_topics?([topic])

      start = Poseidon.timestamp_ms
      remaining_wait_ms = @metadata_fetch_timeout_ms
      while !@cluster_metadata.have_metadata_for_topics?([topic])
        version = @cluster_metadata.version
        @cluster_metadata.add_topic(topic)
        @cluster_metadata.request_update
        puts "Wakeup from metadata loop"
        @sender.wakeup
        @cluster_metadata.await_update(version, remaining_wait_ms) 

        elapsed = Poseidon.timestamp_ms - start
        if elapsed > @metadata_fetch_timeout_ms
          raise "METADTATA FALIURESAF"
        end

        remaining_wait_ms = @metadata_fetch_timeout_ms - elapsed
      end
    end

    def refresh_interval_elapsed?
      @cluster_metadata.last_refreshed_at.nil? ||
        (Time.now - @cluster_metadata.last_refreshed_at) > @metadata_refresh_interval_ms
    end
  end
end
