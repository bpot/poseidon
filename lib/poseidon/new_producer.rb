require 'concurrent/ivar'
module Poseidon
  class NewProducer
    def initialize(client_id, seed_brokers, options = {})
      @partitioner = nil
      @socket_timeout_ms = 10_0000
      @metadata_refresh_interval_ms = 600_000

      @client_id          = client_id
      @cluster_metadata   = ClusterMetadata.new
      @message_conductor  = MessageConductor.new(@cluster_metadata, @partitioner)

      @cluster_metadata.add_seed_brokers(seed_brokers)

      @selector = Selector.new
      @client = NetworkClient.new(@selector, @client_id, @cluster_metadata)
      @record_accumulator = RecordAccumulator.new
      @sender = ProducerSender.new(@client, @cluster_metadata, @record_accumulator)

    end

    def send_message(message_to_send, &cb)
      wait_on_metadata(message_to_send.topic)

      if refresh_interval_elapsed?
        refresh_metadata(message_to_send.topic)
      end

      #pp @cluster_metadata

      partition_id, _ = @message_conductor.destination(message_to_send.topic, message_to_send.key)

      puts "Sending to #{partition_id}"
      future = @record_accumulator.add(message_to_send.topic, message_to_send.key, message_to_send.value, partition_id, compression = nil, cb)
      future 
    end

    def close
      @sender.initiate_close
      @sender.join
    end

    private
    def refresh_metadata(topic)
      @cluster_metadata.add_topic(topic)
      @cluster_metadata.update(@broker_pool.fetch_metadata(@cluster_metadata.topics_of_interest))
      @broker_pool.update_known_brokers(@cluster_metadata.brokers)
    end

    def wait_on_metadata(topic)
      while !@cluster_metadata.have_metadata_for_topics?([topic])
        @cluster_metadata.add_topic(topic)
        @cluster_metadata.request_update
        @sender.wakeup
      end
    end

    def ensure_metadata_available_for_topic(topic)
      while !@cluster_metadata.have_metadata_for_topics?([topic])
        #puts "Refreshing metadata"
        #pp @cluster_metadata
        refresh_metadata(topic)
        sleep 10
      end
    end

    def refresh_interval_elapsed?
      @cluster_metadata.last_refreshed_at.nil? ||
        (Time.now - @cluster_metadata.last_refreshed_at) > @metadata_refresh_interval_ms
    end
  end
end
