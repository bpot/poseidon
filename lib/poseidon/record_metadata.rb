module Poseidon
  class RecordMetadata
    attr_reader :offset, :topic, :partition
    def initialize(topic_partition, base_offset, relative_offset)
      @topic = topic_partition.topic
      @partition = topic_partition.partition
      @offset = base_offset + relative_offset
    end
  end
end
