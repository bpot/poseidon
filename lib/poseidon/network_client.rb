module Poseidon
  class NetworkClient
    def initialize(selector, client_id, cluster_metadata)
      @selector = selector
      @client_id = client_id
      @cluster_metadata = cluster_metadata

      @connection_states = {}
      @in_flight_requests = InFlightRequests.new
    end

    def ready(broker)
      if sendable?(broker)
        return true
      end

      if @connection_states[broker.id].nil? # TODO check for backoff
        @selector.connect(broker.id, broker.host, broker.port)
        @connection_states[broker.id] = :connecting
      end

      return false
    end

    def poll(client_requests)
      p "POLLING IN NETWORK CLIENT WITH: #{client_requests.inspect}"
      network_sends = []
      client_requests.each do |client_request|
        network_sends << NetworkSend.new(client_request.send)
        @in_flight_requests.add(client_request)
      end

      maybe_update_metadata(network_sends)

      @selector.poll(network_sends)

      responses = []

      @selector.connected.each do |broker_id|
        puts "YO WE CONNECTED TO #{broker_id}"
        @connection_states[broker_id] = :connected
      end

      @selector.disconnected.each do |broker_id|
        @connection_states[broker_id] = :disconnected
        
        # Cancel any inflight requests
        requests = @in_flight_requests.clear_all(broker_id)
        p "CANCELING INFLIGHT REQUESTS: #{requests.inspect} -- #{@in_flight_requests.inspect}"
        requests.each do |request|
          responses << ClientResponse.new(request, true, nil)
        end
      end

      if @selector.disconnected.any?
        # REFRESH METADATA
      end

      @selector.completed_receives.each do |completed_receive|
        puts "COMPLETED RECEIVE: #{completed_receive.inspect}"
        request_to_complete = @in_flight_requests.complete_next(completed_receive.source)
        puts "REQUEST TO COMPLETE: #{request_to_complete}"

        response_buffer = Protocol::ResponseBuffer.new(completed_receive.buffer)
        response_header = Protocol::ResponseCommon.read(response_buffer)
        api_key = request_to_complete.send.struct.common.api_key
        puts "API_KEY: #{api_key}"
        if response_header.correlation_id != request_to_complete.send.struct.common.correlation_id
          raise "Correlation ids don't match!"
        end
        responses << ClientResponse.new(request_to_complete, false, Protocol::ResponseBuffer.new(completed_receive.buffer))
      end
      responses
    end

    def next_request_header(request_type)
      Protocol::RequestCommon.new(
        Protocol::API_KEYS[request_type],
        #API_VERSION,
        0,
        next_correlation_id,
        @client_id
      )
    end

    private
    def maybe_update_metadata(sends)
      node = least_loaded_node
      if node.nil?
        raise "Uhhh cantr handle this"
      end

      if @connection_states[node.id] == :connected
        puts "Adding metadata command"
        metadata_fetch_request = Protocol::MetadataRequest.new(next_request_header(:metadata), @cluster_metadata.topics)
        request_send = RequestSend.new(node.id, metadata_fetch_request)
        client_request = ClientRequest.new(request_send)
        sends << NetworkSend.new(request_send)
        @in_flight_requests.add(client_request)
      else #if can connected?
        # start connect!
        @selector.connect(node.id, node.host, node.port)
      end
    end

    # XXX stub
    def least_loaded_node
      @cluster_metadata.brokers.values.first 
    end

    def sendable?(broker)
      @connection_states[broker.id] == :connected # && TODO check if there are too many in flight requests
    end

    def next_correlation_id
      @correlation_id ||= 0
      @correlation_id  += 1
    end
  end
end
