module Poseidon
  class NetworkClient
    def initialize(selector, client_id, cluster_metadata)
      @selector = selector
      @client_id = client_id
      @cluster_metadata = cluster_metadata

      @in_flight_requests = InFlightRequests.new
      @connection_states = {}
      @metadata_fetch_in_progress = false
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
      network_sends = []
      client_requests.each do |client_request|
        puts "[#{Poseidon.timestamp_ms}] Sending: #{client_request.inspect}"
        network_sends << NetworkSend.new(client_request.request)
        @in_flight_requests.add(client_request)
      end

      time_to_next_metadata_update = @cluster_metadata.time_to_next_update
      #time_to_next_reconnect_attempt = @last_no_node_available_ms + @cluster_metadata.refresh_backoff_ms
      time_to_next_reconnect_attempt = 0
      metadata_timeout = [time_to_next_metadata_update, time_to_next_reconnect_attempt].max

      puts "[#{Poseidon.timestamp_ms}] Metadata Time: #{metadata_timeout}"
      if !@metadata_fetch_in_progress && metadata_timeout == 0
        maybe_update_metadata(network_sends)
      end

      @selector.poll(network_sends)

      responses = []

      @selector.connected.each do |broker_id|
        @connection_states[broker_id] = :connected
      end

      @selector.disconnected.each do |broker_id|
        @connection_states[broker_id] = :disconnected

        # Cancel any inflight requests
        requests = @in_flight_requests.clear_all(broker_id)
        requests.each do |request|
          api_key = Protocol.api_key_for_id(request.struct.common.api_key)
          if api_key == :produce
            @metadata_fetch_in_progress = false
          else
            responses << ClientResponse.new(request, true, nil)
          end
        end
      end

      if @selector.disconnected.any?
        # XXX REFRESH METADATA
      end

      @selector.completed_receives.each do |completed_receive|
        request_to_complete = @in_flight_requests.complete_next(completed_receive.source)

        response_buffer = Protocol::ResponseBuffer.new(completed_receive.buffer)
        response_header = Protocol::ResponseCommon.read(response_buffer)
        api_key = Protocol.api_key_for_id(request_to_complete.request.struct.common.api_key)
        if response_header.correlation_id != request_to_complete.request.struct.common.correlation_id
          raise "Correlation ids don't match!"
        end

        if api_key == :metadata
          handle_metadata_response(Protocol::ResponseBuffer.new(completed_receive.buffer))
        else
          responses << ClientResponse.new(request_to_complete, false, Protocol::ResponseBuffer.new(completed_receive.buffer))
        end
      end
      responses
    end

    def handle_metadata_response(response_buffer)
      @metadata_fetch_in_progress = false

      metadata_response = Protocol::MetadataResponse.read(response_buffer)
      if metadata_response.brokers.any?
        @cluster_metadata.update(metadata_response)
      end
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

    def wakeup
      @selector.wakeup
    end

    private
    def maybe_update_metadata(sends)
      node = least_loaded_node
      if node.nil?
        raise "Uhhh cantr handle this"
      end

      if @connection_states[node.id] == :connected
        puts "[#{Poseidon.timestamp_ms}] Sending metadata command"
        metadata_fetch_request = Protocol::MetadataRequest.new(next_request_header(:metadata), @cluster_metadata.topics_of_interest)
        request_send = RequestSend.new(node.id, metadata_fetch_request)
        client_request = ClientRequest.new(request_send)
        sends << NetworkSend.new(request_send)
        @in_flight_requests.add(client_request)
        @metadata_fetch_in_progress = true
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
