module Poseidon
  # BrokerPool allows you to send api calls to the a brokers Connection.
  # 
  # @api private
  class BrokerPool
    class UnknownBroker < StandardError; end

    # @param [String] client_id
    def initialize(client_id, seed_brokers)
      @connections = {}
      @brokers = {}
      @client_id = client_id
      @seed_brokers = seed_brokers
    end

    def fetch_metadata(topics)
      @seed_brokers.each do |broker|
        if metadata = fetch_metadata_from_broker(broker, topics)
          Poseidon.logger.debug { "Fetched metadata\n" + metadata.to_s }
          return metadata
        end
      end
      raise Errors::UnableToFetchMetadata
    end

    # Update the brokers we know about
    #
    # TODO break connection when a brokers info changes?
    #
    # @param [Hash<Integer,Hash>] brokers
    #   Hash of broker_id => { :host => host, :port => port }
    def update_known_brokers(brokers)
      @brokers.update(brokers)
      nil
    end

    # Executes an api call on the connection
    #
    # @param [Integer] broker_id id of the broker we want to execute it on
    # @param [Symbol] api_call
    #   the api call we want to execute (:produce,:fetch,etc)
    def execute_api_call(broker_id, api_call, *args)
      connection(broker_id).send(api_call, *args)
    end

    # Closes all open connections to brokers
    def shutdown
      @brokers.values(&:close)
      @brokers = {}
    end

    private
    def fetch_metadata_from_broker(broker, topics)
      host, port = broker.split(":")
      c = Connection.new(host, port, @client_id)
      c.topic_metadata(topics)
    rescue Connection::ConnectionFailedError
      return nil
    end

    def connection(broker_id)
      @connections[broker_id] ||= new_connection(broker_id)
    end

    def new_connection(broker_id)
      info = @brokers[broker_id]
      if info.nil?
        raise UnknownBroker
      end
      Connection.new(info[:host], info[:port], @client_id)
    end
  end
end
