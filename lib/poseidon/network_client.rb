module Poseidon
  class NetworkClient
    def initialize(selector, client_id, cluster_metadata, reconnect_backoff_ms)
      @selector = selector
      @client_id = client_id
      @cluster_metadata = cluster_metadata
      @reconnect_backoff_ms = reconnect_backoff_ms

      @in_flight_requests = InFlightRequests.new
      @connection_states = {}
      @connection_attempted_at = {}
      @metadata_fetch_in_progress = false
      @last_no_node_available_ms = 0
    end

    def ready(node)
      if sendable?(node)
        return true
      end

      if !node_blacked_out?(node.id)
        initiate_connect(node)
      end

      return false
    end

    def poll(client_requests, timeout)
      network_sends = []
      client_requests.each do |client_request|
        #puts "[#{Poseidon.timestamp_ms}] Sending: #{client_request.inspect}"
        network_sends << NetworkSend.new(client_request.request)
        @in_flight_requests.add(client_request)
      end

      time_to_next_metadata_update = @cluster_metadata.time_to_next_update
      #puts "[#{Poseidon.timestamp_ms}] Time To Next Update #{time_to_next_metadata_update}"
      time_to_next_reconnect_attempt = [@last_no_node_available_ms + @cluster_metadata.refresh_backoff_ms - Poseidon.timestamp_ms, 0].max
      wait_for_metadata_fetch = @metadata_fetch_in_progress ? INT_MAX : 0
      metadata_timeout = [time_to_next_metadata_update, time_to_next_reconnect_attempt, wait_for_metadata_fetch].max

      #puts "[#{Poseidon.timestamp_ms}] Metadata Timeout: #{metadata_timeout}"
      if !@metadata_fetch_in_progress && metadata_timeout == 0
        maybe_update_metadata(network_sends)
      end

      #puts "[NetworkClient] min(#{timeout}, #{metadata_timeout})"
      poll_timeout = [timeout, metadata_timeout].min
      @selector.poll(poll_timeout, network_sends)

      responses = []

      @selector.connected.each do |broker_id|
        @connection_states[broker_id] = :connected
      end

      @selector.disconnected.each do |broker_id|
        puts "[#{Poseidon.timestamp_ms}] Handling disconnected node: #{broker_id}"
        @connection_states[broker_id] = :disconnected

        # Cancel any inflight requests
        client_requests = @in_flight_requests.clear_all(broker_id)
        client_requests.each do |request|
          #puts "Handling request in flight during disconnect: #{request.inspect}"
          api_key = Protocol.api_key_for_id(request.request.struct.common.api_key)
          #puts "API KEY: #{api_key}"
          if api_key == :metadata
            #puts "Metadata cancel!"
            @metadata_fetch_in_progress = false
          else
            #puts "Canceling requests because node disconnected"
            responses << ClientResponse.new(request, true, nil)
          end
        end
      end

      if @selector.disconnected.any?
        @cluster_metadata.request_update
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
      #if metadata_response.brokers.any?
        @cluster_metadata.update(metadata_response)
      #else
      #  puts "[#{Poseidon.timestamp_ms}] Not updating metadata because no brokers (#{metadata_response.inspect})"
      #end
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

    def connection_delay(node)
      if (state = @connection_states[node.id].nil?)
        return 0
      end

      time_waited = Poseidon.timestamp_ms - @connection_attempted_at[node.id]
      if state == :disconnected
        return [@reconnect_backoff_ms, 0].max
      else
        return LONG_MAX
      end
    end

    private
    def initiate_connect(node)
      @connection_states[node.id] = :connecting

      @selector.connect(node.id, node.host, node.port)
      @connection_attempted_at[node.id] = Poseidon.timestamp_ms
    end

    def maybe_update_metadata(sends)
      node = least_loaded_node
      if node.nil?
        #puts "[#{Poseidon.timestamp_ms}] No least loaded node available"
        @last_no_node_available_ms = Poseidon.timestamp_ms
        return
      end

      if @connection_states[node.id] == :connected
        #puts "[#{Poseidon.timestamp_ms}] Sending metadata command"
        metadata_fetch_request = Protocol::MetadataRequest.new(next_request_header(:metadata), @cluster_metadata.topics_of_interest)
        request_send = RequestSend.new(node.id, metadata_fetch_request)
        client_request = ClientRequest.new(request_send)
        sends << NetworkSend.new(request_send)
        @in_flight_requests.add(client_request)
        @metadata_fetch_in_progress = true
      elsif !node_blacked_out?(node.id)
        initiate_connect(node)
        @last_no_node_available_ms = Poseidon.timestamp_ms
      else
        @last_no_node_available_ms = Poseidon.timestamp_ms
      end
    end

    def least_loaded_node
      in_flight = nil
      found = nil

      nodes = @cluster_metadata.brokers.values.shuffle
      nodes.each do |node|
        curr_in_flight = @in_flight_requests.in_flight_request_count(node.id)
        if curr_in_flight == 0 && @connection_states[node.id] &&  @connection_states[node.id] == :connected
          return node
        elsif !node_blacked_out?(node.id) && (in_flight.nil? || curr_in_flight < in_flight)
          found = node
          in_flight = curr_in_flight
        end
      end

      found
    end

    def node_blacked_out?(node_id)
      state = @connection_states[node_id]
      if state.nil?
        return false
      else
        return state == :disconnected && (Poseidon.timestamp_ms - @connection_attempted_at[node_id] < @reconnect_backoff_ms)
      end
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
