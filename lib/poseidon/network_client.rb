module Poseidon
  class NetworkClient
    def initialize(selector, client_id)
      @selector = selector
      @client_id = client_id
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
      network_sends = []
      client_requests.each do |client_request|
        network_sends << NetworkSend.new(client_request.send)
        @in_flight_requests.add(client_request)
      end

      @selector.poll(network_sends)

      @selector.connected.each do |broker_id|
        puts "YO WE CONNECTED TO #{broker_id}"
        @connection_states[broker_id] = :connected
      end

      responses = []
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
        responses << ClientResponse.new(request_to_complete, Protocol::ResponseBuffer.new(completed_receive.buffer))
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
    def sendable?(broker)
      @connection_states[broker.id] == :connected # && TODO check if there are too many in flight requests
    end

    def next_correlation_id
      @correlation_id ||= 0
      @correlation_id  += 1
    end
  end
end
