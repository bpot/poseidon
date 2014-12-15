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
      @client.wakeup
    end

    def run
      puts "[#{Poseidon.timestamp_ms}] Top of sender loop"
      ready_result = @record_accumulator.ready(@cluster_metadata)
      #puts "READY RESULT: #{ready_result.inspect}"
      # TODO force metadata refresh if we need to

      not_ready_timeout = LONG_MAX
      ready_result.ready_nodes.select! do |broker|
        if !@client.ready(broker)
          puts "[#{Poseidon.timestamp_ms}] Removing broker client says is not ready"
          not_ready_timeout = [not_ready_timeout, @client.connection_delay(broker)].min
          false
        else
          true
        end
      end

      if ready_result.unknown_leaders_exist
        #puts "UNKNOWN LEADERS"
        @cluster_metadata.request_update
      end

      #p "READY: #{ready.inspect}"

      record_batches_by_broker = @record_accumulator.drain(@cluster_metadata, ready_result.ready_nodes)
      #puts "DRAINED: #{record_batches_by_broker.inspect}\n#{@record_accumulator.inspect}"
      requests = []
      if record_batches_by_broker && record_batches_by_broker.any?
        record_batches_by_broker.each do |broker_id, record_batches|

          messages_for_topics = []
          batches_by_topic = record_batches.group_by(&:topic)
          batches_by_topic.each do |topic, batches_for_topic|
            messages_for_partitions = []
            batches_for_topic_by_partition = batches_for_topic.group_by(&:partition)
            batches_for_topic_by_partition.each do |partition, batches_for_partition|
              #message_set = MessageSet.new
              batches_for_partition.each do |batch|
                #message_set << batch.to_s
                #batch.records.each do |record|
                #  message_set << Message.new(value: record[:value], key: record[:key])
                #end
                #pp "MESSAGE SET: #{batch.message_set.inspect}"
                messages_for_partitions << Protocol::MessagesForPartition.new(partition, batch.message_set)
              end
            end
            messages_for_topics << Protocol::MessagesForTopic.new(topic, messages_for_partitions)
          end

          # XXX gets acks from somewhere
          producer_request_for_broker = Protocol::ProduceRequest.new(@client.next_request_header(:produce), 1, 30000, messages_for_topics)
          requests << ClientRequest.new(RequestSend.new(broker_id, producer_request_for_broker), record_batches.group_by(&:topic_partition))
        end
      end

      poll_timeout = [ready_result.next_ready_check_delay_ms, not_ready_timeout].min
        
      #$pp "REQUESTS: #{requests.inspect}"
      responses = @client.poll(requests, poll_timeout)
      responses.each do |response|
        # XXX we need to support retries for record batches and stuff here
        if response.disconnected
          pp "HANDLE DISCONNECT: #{response.inspect}"
          record_batches_by_topic_partition = response.request.attachment
          record_batches_by_topic_partition.values.each do |record_batch|
            record_batch.done(-1, Errors::NetworkException)
          end
        else
          produce_response = Protocol::ProduceResponse.read(response.response)
          #pp "PRODUCE_RESPONSE: #{produce_response.inspect}"
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
