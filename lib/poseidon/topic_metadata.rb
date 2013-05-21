module Poseidon
  # @api private
  class TopicMetadata
    # Build a new TopicMetadata object from its binary representation
    #
    # @param [ResponseBuffer] buffer
    # @return [TopicMetadata]
    #
    def self.read(buffer)
      tm = TopicMetadata.new
      tm.struct = Protocol::TopicMetadataStruct.read(buffer)
      tm
    end

    attr_accessor :struct
    def initialize(struct=nil)
      self.struct = struct
    end

    # Write a binary representation of the TopicMetadata to buffer
    # 
    # @param [RequestBuffer] buffer
    # @return [nil]
    def write(buffer)
      struct.write(buffer)
      nil
    end

    def partitions
      struct.partitions
    end

    def name
      struct.name
    end

    def ==(o)
      eql?(o)
    end

    def eql?(o)
      struct.eql?(o.struct)
    end

    def objects_with_errors
      struct.objects_with_errors
    end

    def leader_available?
      struct.error_class != Errors::LeaderNotAvailable
    end

    def partition_count
      @partition_count ||= struct.partitions.count
    end

    def available_partition_leader_ids
      @available_partition_leader_ids ||= struct.partitions.select(&:leader)
    end

    def available_partition_count
      @available_partition_count ||= available_partition_leader_ids.count
    end
  end
end
