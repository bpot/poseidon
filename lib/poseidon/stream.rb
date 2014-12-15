module Poseidon
  class Stream
    def initialize(io, sockaddr)
      @io = io
      @sockaddr = sockaddr
      @state = :connecting
      @sends = []

      @to_read = 0
      @read_buffer = ""
    end

    def to_io
      @io
    end

    def <<(send)
      @sends << send
    end

    def read?
      false
    end

    def write?
      @state == :connecting || (@state == :connected && @sends.any?)
    end

    def handle_write
      if @state == :connecting
        begin
          # XXX can we use getsockopt here?
          @io.connect_nonblock(@sockaddr)
          @state = :connected
          #p "CONNECTED"
          return :connected
        rescue IO::WaitWritable
          p "HANDLE READ: #{$!.inspect}"
        rescue StandardError
          p "HANDLE READ ERRR: #{$!.inspect}"
        end
      elsif @state == :connected && @sends.any?
        send = @sends.first
        request_buffer = Protocol::RequestBuffer.new
        #pp send
        send.request.struct.write(request_buffer)

        message = [request_buffer.to_s.bytesize].pack("N") + request_buffer.to_s

        written = @io.write_nonblock(message)
        #puts "Wrote #{written} of #{request_buffer.to_s.bytesize}"
        if written == request_buffer.to_s.bytesize + 4
          @sends.shift
        else
          raise "ZOMG can't handle partial writes yet yo"
        end
      end
    end

    def handle_read
      completed = []

      read = @io.read_nonblock(16384)


      loop do
        if @to_read == 0
          @to_read = read.slice!(0,4).unpack("N").first
        end
        #p "TO READ: #{@to_read}"

        slice = read.slice!(0,@to_read)
        @read_buffer << slice
        @to_read -= slice.size
        #p "TO READ AFTER: #{@to_read}"

        if @to_read < 0
          raise "ZOMG THIS SHOULD NEVER HAPPEN"
        end

        if @to_read == 0
          completed << @read_buffer
          @read_buffer = ""
        end

        #p "READ SIZE: #{read.size}"
        break if read.size == 0
      end


      completed
    end

    private
    def consume_bytes(read)
    end
  end
end
