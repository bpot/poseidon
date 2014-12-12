module Poseidon
  class Selector
    include Socket::Constants

    def initialize
      @streams = {}
      @streams_inverted = {}

      @completed_sends = []
      @completed_receives = []
      @connected = []
      @disconnected = []
    end

    def connect(broker_id, host, port)
      sock = Socket.new(AF_INET, SOCK_STREAM, 0)
      sockaddr = Socket.sockaddr_in(port, host)
      begin
        sock.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        p $!
      end

      @streams[broker_id] = Stream.new(sock, sockaddr)
      @streams_inverted[@streams[broker_id]] = broker_id
    end

    def disconnect(broker_id)
      connection = @socks.delete(broker_id)
      if connection
        @socks_inverted.delete(connection)
        connection.close
      end
    end

    def wakeup
    end

    def close
    end

    def poll(sends)
      writes = @streams.values.select(&:write?)
      can_read, can_write, = IO.select(nil, writes, nil, 10)
      pp can_read
      pp can_write
      can_write.each do |writable|
        case writable.handle_write
        when :connected
          @connected << writable
        else
          p "Meh"
        end
      end
#      clear
      
=begin
      writes = sends.keys.map { |broker_id| @socks[broker_id] }
      # XXX raise error if writes has a nil
      #
      reads = @socks.keys

      can_read, can_write, _ = IO.select(reads, writes, [], 1)
      if can_read
        can_read.each do |io|
        end
      end

      if can_write
        can_write.each do |io|
          broker_id = @socks_inverted[io]
          to_send = sends[broker_id]
          io.write(to_send)
        end
      end
=end
    end

    def completed_sends
    end

    def completed_receives
    end

    def disconnected
    end

    def connected
    end

    private
    def clear
    end
  end
end
