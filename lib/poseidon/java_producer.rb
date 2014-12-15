module Poseidon
  class JavaProducer
    JAR_PATH = "/home/bpotter/src/gh/apache/kafka/clients/build/libs/kafka-clients-0.8.2-beta.jar"
    require 'java'
    require JAR_PATH
    Dir["/home/bpotter/src/gh/apache/kafka/core/build/dependant-libs-2.11/*.jar"].each do |p|
      p p
      require p
    end
    java_import org.apache.kafka.clients.producer.KafkaProducer
    java_import org.apache.kafka.clients.producer.ProducerRecord
    java_import java.util.Properties

    def initialize(client_id, seed_brokers, options = {})
      props = Properties.new
      props.put("bootstrap.servers", seed_brokers.join(","))
      props.put("client.id", client_id)
      @producer = KafkaProducer.new(props)
    end

    def send_message(message_to_send, &cb)
      future = @producer.send(ProducerRecord.new(message_to_send.topic.to_java, message_to_send.value.to_java_bytes))
=begin
      wait_on_metadata(message_to_send.topic)

      partition_id, _ = @message_conductor.destination(message_to_send.topic, message_to_send.key)

      puts "[#{Poseidon.timestamp_ms}] Appending record"
      append_result = @record_accumulator.append(message_to_send.topic, message_to_send.key, message_to_send.value, partition_id, compression = nil, cb)
      puts "[#{Poseidon.timestamp_ms}] Record appeneded"

      # XXX HAX!
      @sender.wakeup

      append_result.future
=end
    end

    def close
      @producer.close
    end
  end
end
