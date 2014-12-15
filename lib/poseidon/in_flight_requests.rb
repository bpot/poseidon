module Poseidon
  class InFlightRequests
    def initialize
      @requests = {}
    end

    def in_flight_request_count(node)
      requests = @requests[node]

      if requests
        requests.size
      else
        0
      end
    end

    def add(client_request)
      dest = client_request.request.destination
      @requests[dest] ||= []
      @requests[dest] << client_request
    end

    def complete_next(node)
      @requests[node].shift
    end

    def clear_all(node)
      @requests.delete(node) || []
    end
  end
end
