module Poseidon
  class Node
    attr_reader :broker_id, :host, :port
    def initialize(broker_id, host, port)
      @broker_id = broker_id
      @host = host
      @port = port
    end

    def eql?(other)
      broker_id == other.broker_id
    end

    def hash
      @broker_id.hash
    end
  end
end
