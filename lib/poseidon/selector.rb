module Poseidon
  class Selector
    include Socket::Constants

    attr_reader :connected, :disconnected, :completed_receives
    def initialize
      @streams = {}
      @streams_inverted = {}

      @completed_sends = []
      @completed_receives = []
      @connected = []
      @disconnected = []

      @wait, @wake = IO.pipe
      #puts "PIPE: #{@wait.inspect} #{@wake.inspect}"
    end

    def connect(broker_id, host, port)
      sock = Socket.new(AF_INET, SOCK_STREAM, 0)
      sockaddr = Socket.sockaddr_in(port, host)
      begin
        sock.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        p "First nonblock: #{$!.inspect}"
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
      puts "Waking up"
    #  puts caller.join("\n")
    #  puts "-----------------------------------------------"
      @wake.write "\0"
    end

    def close
    end

    def poll(poll_timeout, network_sends)
      clear

      #puts "NETWORK SENDS"
      #pp network_sends

      requests_to_write = {}
      network_sends.each do |send|
        stream = @streams[send.destination]
        stream << send
      end

      reads = @streams.values#.select(&:read?)
      writes = @streams.values.select(&:write?)
      can_read, can_write, = IO.select(reads + [@wait], writes, nil, poll_timeout / 1000.0)
      puts "[#{Poseidon.timestamp_ms}] Select finished: #{can_read.inspect} to read, #{can_write.inspect} to write, timeout: #{poll_timeout/1000.0}"
      #pp can_read
      #pp can_write
      if can_write
        can_write.each do |writable|
          case writable.handle_write
          when :connected
            @connected << @streams_inverted[writable]
          else
            #p "Meh"
          end
        end
      end

      if can_read
        can_read.each do |readable|
          if readable == @wait
            # XXX need to handle case where more than 4096 wakeups have been triggered!?
            @wait.read_nonblock(4096)
            next
            # Return here?
          end

          begin
            completed = readable.handle_read
            @completed_receives += completed.map { |buffer| NetworkReceive.new(@streams_inverted[readable], buffer) }
          rescue EOFError, Errno::ENOTCONN, Errno::ECONNRESET
            #puts "DISCONNECTED: #{readable}"
            # Need to do anything with the stream here?!
            # What if there are things to send in the buffer?!
            broker_id = @streams_inverted[readable]
            @disconnected << broker_id

            @streams_inverted.delete(readable)
            @streams.delete(broker_id)
          end
        end
      end
    end

    def completed_sends
    end

    private
    def clear
      @connected = []
      @completed_receives = []
      @disconnected = []
    end
  end
end
