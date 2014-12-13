module Poseidon
  class ProducerSender
    def initialize(client, cluster_metadata, record_accumulator)
      @client = client
      @cluster_metadata = cluster_metadata
      @record_accumulator = record_accumulator

      @running = true
      @sender_thread = start_sender_thread
    end

    def start_sender_thread
      Thread.new {
        begin
          while @running
            run
          end
        rescue Exception
          p $!
          puts $!.backtrace.join("\n")
          raise
        end
      }
    end

    def initiate_close
      @running = false
      @record_accumulator.close
      wakeup
    end

    def join
      @sender_thread.join
    end

    def wakeup
      # ???
    end

    def run
      ready = @record_accumulator.ready(@cluster_metadata)
      # TODO force metadata refresh if we need to

      ready.select! do |broker|
        @client.ready(broker)
      end

      p "READY: #{ready.inspect}"

      record_batches_by_broker = @record_accumulator.drain(@cluster_metadata, ready)
      requests = []
      if record_batches_by_broker && record_batches_by_broker.any?
        record_batches_by_broker.each do |broker_id, record_batches|

          messages_for_topics = []
          batches_by_topic = record_batches.group_by(&:topic)
          batches_by_topic.each do |topic, batches_for_topic|
            messages_for_partitions = []
            batches_for_topic_by_partition = batches_for_topic.group_by(&:partition)
            batches_for_topic_by_partition.each do |partition, batches_for_partition|
              message_set = MessageSet.new
              batches_for_partition.each do |batch|
                batch.records.each do |record|
                  message_set << Message.new(value: record[:value], key: record[:key])
                end
              end
              messages_for_partitions << Protocol::MessagesForPartition.new(partition, message_set)
            end
            messages_for_topics << Protocol::MessagesForTopic.new(topic, messages_for_partitions)
          end

          # XXX gets acks from somewhere
          producer_request_for_broker = Protocol::ProduceRequest.new(@client.next_request_header(:produce), 1, 30000, messages_for_topics)
          requests << ClientRequest.new(RequestSend.new(broker_id, producer_request_for_broker), record_batches.group_by(&:topic_partition))
        end
      end
        
      responses = @client.poll(requests)
      responses.each do |response|
        if response.disconnected
          pp response.request.attachment
          raise "Not handling disconnect"
        else
          produce_response = Protocol::ProduceResponse.read(response.response)
          record_batches_by_topic_partition = response.request.attachment
          produce_response.topic_response.each do |topic_response|
            topic_response.partitions.each do |partition_response|
              topic_partition = TopicPartition.new(topic_response.topic, partition_response.partition)
              # XXX first?
              record_batch = record_batches_by_topic_partition[topic_partition].first
              record_batch.done(partition_response.offset)
            end
          end
        end
      end
    end
  end
end
