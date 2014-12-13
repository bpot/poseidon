module Poseidon
  class NetworkSend < Struct.new(:request)
    def destination
      request.destination
    end
  end
end
