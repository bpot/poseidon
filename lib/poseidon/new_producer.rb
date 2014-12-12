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
      @broker_pool        = BrokerPool.new(client_id, seed_brokers, @socket_timeout_ms)

      @selector = Selector.new
      @client = NetworkClient.new(@selector)
      @record_accumulator = RecordAccumulator.new
      @sender = ProducerSender.new(@client, @cluster_metadata, @record_accumulator)
    end

    def send_message(message_to_send)
      ivar = Concurrent::IVar.new

      if refresh_interval_elapsed?
        refresh_metadata(message_to_send.topic)
      end

      ensure_metadata_available_for_topic(message_to_send.topic)

      pp @cluster_metadata

      partition_id, _ = @message_conductor.destination(message_to_send.topic, message_to_send.key)

      puts "Sending to #{partition_id}"
      @record_accumulator.add(message_to_send.topic, message_to_send.key, message_to_send.value, partition_id)

      pp @record_accumulator.records_by_broker_id(@cluster_metadata)

      #@sender.run

      ivar
    end

    private
    def refresh_metadata(topic)
      @cluster_metadata.add_topic(topic)
      @cluster_metadata.update(@broker_pool.fetch_metadata(@cluster_metadata.topics_of_interest))
      @broker_pool.update_known_brokers(@cluster_metadata.brokers)
    end

    def ensure_metadata_available_for_topic(topic)
      while !@cluster_metadata.have_metadata_for_topics?([topic])
        puts "Refreshing metadata"
        pp @cluster_metadata
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
