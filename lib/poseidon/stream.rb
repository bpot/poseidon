module Poseidon
  class Stream
    def initialize(io, sockaddr)
      @io = io
      @sockaddr = sockaddr
      @state = :connecting
      @to_write = ""
    end

    def to_io
      @io
    end

    def <<(data)
      @to_write << data
    end

    def read?
      false
    end

    def write?
      @state == :connecting || (@state == :connected && @to_write.size > 0)
    end

    def handle_write
      if @state == :connecting
        begin
          @io.connect_nonblock(@sockaddr)
          @state == :connected
          p "CONNECTED"
          return :connected
        rescue IO::WaitWritable
          p $!
        rescue StandardError
          p $!
        end
      end
    end

    def handle_read
    end
  end
end
