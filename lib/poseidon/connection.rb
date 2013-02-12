module Poseidon
  # High level internal interface to a remote broker. Provides access to
  # the broker API.
  # @api private
  class Connection
    include Protocol

    class ConnectionFailedError < StandardError; end

    API_VERSION = 0
    REPLICA_ID = -1 # Replica id is always -1 for non-brokers

    # Create a new connection
    #
    # @param [String] host Host to connect to
    # @param [Integer] port Port broker listens on
    # @param [String] client_id Unique across processes?
    def initialize(host, port, client_id)
      begin
        @socket = TCPSocket.new(host, port)
      rescue SystemCallError
        raise ConnectionFailedError
      end

      @client_id = client_id
    end

    # Close broker connection
    def close
      @socket.close
    end
  
    # Execute a produce call
    #
    # @param [Integer] required_acks
    # @param [Integer] timeout
    # @param [Array<Protocol::MessagesForTopics>] messages_for_topics Messages to send
    # @return [ProduceResponse]
    def produce(required_acks, timeout, messages_for_topics)
      req = ProduceRequest.new( request_common(:produce),
                                required_acks,
                                timeout,
                                messages_for_topics) 
      send_request(req)
      read_response(ProduceResponse)
    end

    # Execute a fetch call
    #
    # @param [Integer] max_wait_time
    # @param [Integer] min_bytes
    # @param [Integer] topic_fetches
    def fetch(max_wait_time, min_bytes, topic_fetches)
      req = FetchRequest.new( request_common(:fetch),
                                REPLICA_ID,
                                max_wait_time,
                                min_bytes,
                                topic_fetches) 
      send_request(req)
      read_response(FetchResponse)
    end

    def offset(offset_topic_requests)
      req = OffsetRequest.new(request_common(:offset),
                              REPLICA_ID,
                              offset_topic_requests)
      send_request(req)
      read_response(OffsetResponse).topic_offset_responses
    end

    # Fetch metadata for +topic_names+
    #
    # @param [Enumberable<String>] topic_names
    #   A list of topics to retrive metadata for
    # @return [TopicMetadataResponse] metadata for the topics
    def topic_metadata(topic_names)
      req = MetadataRequest.new( request_common(:metadata),
                                 topic_names)
      send_request(req)
      read_response(MetadataResponse)
    end

    private
    def read_response(response_class)
      n = @socket.read(4).unpack("N").first
      s = @socket.read(n)
      buffer = Protocol::ResponseBuffer.new(s)
      response_class.read(buffer)
    end

    def send_request(request)
      buffer = Protocol::RequestBuffer.new
      request.write(buffer)
      @socket.write([buffer.to_s.size].pack("N") + buffer.to_s)
    end

    def request_common(request_type)
      RequestCommon.new(
        API_KEYS[request_type],
        API_VERSION,
        next_correlation_id,
        @client_id
      )
    end

    def next_correlation_id
      @correlation_id ||= 0
      @correlation_id  += 1
    end
  end
end
