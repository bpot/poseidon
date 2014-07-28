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

    def name
      struct.name
    end

    def ==(o)
      eql?(o)
    end

    def exists?
      struct.error == 0
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

    def available_partitions
      @available_partitions ||= struct.partitions.select do |partition|
        partition.error == 0 && partition.leader != -1
      end
    end

    def available_partition_count
      available_partitions.count
    end

    def partition_leader(partition_id)
      partition = partitions_by_id[partition_id]
      if partition
        partition.leader
      else
        nil
      end
    end

    def to_s
      struct.partitions.map { |p| p.inspect }.join("\n")
    end

    private
    def partitions_by_id
      @partitions_by_id ||= Hash[partitions.map { |p| [p.id, p] }]
    end

    def partitions
      struct.partitions
    end
  end
end
