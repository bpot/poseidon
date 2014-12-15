# Stdlib requires
require 'socket'
require 'zlib'
require 'thread'
require 'set'
require 'logger'
require 'stringio'

require 'timestamp'
require 'active_support/notifications'

trap('TTIN') do
  Thread.list.each do |t|
    puts "---------------------------------------------"
    puts t.backtrace.join("\n")
  end
end

# Top level Poseidon namespace
#
# @api public
module Poseidon
  LONG_MAX = 9223372036854775807
  INT_MAX = 2147483647

  def self.timestamp_ms
    (Time.unix_microtime * 1000).to_i
  end

  # Posiedon exception namespace
  module Errors
    # @api private
    class ProtocolError < StandardError; end

    # @api private
    class RetriableProtocolError < ProtocolError; end

    # Protocol Exceptions
    #
    # These are defined by the Poseidon wire format,
    # they should be caught before being raised to users.
    #
    # @api private
    class UnknownError < ProtocolError; end
    # @api private
    class OffsetOutOfRange < ProtocolError; end
    # @api private
    class InvalidMessage < ProtocolError; end
    # @api private
    class UnknownTopicOrPartition < RetriableProtocolError; end
    # @api private
    class InvalidMessageSize < ProtocolError; end
    # @api private
    class LeaderNotAvailable < ProtocolError; end
    # @api private
    class NotLeaderForPartition < ProtocolError; end
    # @api private
    class RequestTimedOut < ProtocolError; end
    # @api private
    class BrokerNotAvailable < ProtocolError; end
    # @api private
    class ReplicaNotAvailable < ProtocolError; end
    # @api private
    class MessageSizeTooLarge < ProtocolError; end
    # @api private
    class StaleControllerEpoch < ProtocolError; end
    # @api private
    class OffsetMetadataTooLarge < ProtocolError; end
    # @api private
    class NetworkException < ProtocolError; end
    # @api private
    class OffsetsLoadInProgress < ProtocolError; end
    # @api private
    class ConsumerCoordinatorNotAvailable < ProtocolError; end
    # @api private
    class NotCoordinatorForConsumer < ProtocolError; end
    # @api private
    class UnrecognizedProtocolError < ProtocolError; end

    # @api private
    NO_ERROR_CODE = 0
    # @api private
    ERROR_CODES = {
      -1 => UnknownError,
      1 => OffsetOutOfRange,
      2 => InvalidMessage,
      3 => UnknownTopicOrPartition,
      4 => InvalidMessageSize,
      5 => LeaderNotAvailable,
      6 => NotLeaderForPartition,
      7 => RequestTimedOut,
      8 => BrokerNotAvailable,
      9 => ReplicaNotAvailable,
      10 => MessageSizeTooLarge,
      11 => StaleControllerEpoch,
      12 => OffsetMetadataTooLarge,
      13 => NetworkException,
      14 => OffsetsLoadInProgress,
      15 => ConsumerCoordinatorNotAvailable,
      16 => NotCoordinatorForConsumer
    }

    # Raised when a custom partitioner tries to send
    # a message to a partition that doesn't exist.
    class InvalidPartitionError < StandardError; end

    # Raised when we are unable to fetch metadata from
    # any of the brokers.
    class UnableToFetchMetadata < StandardError; end

    # Raised when a messages checksum doesn't match
    class ChecksumError < StandardError; end

    # Raised when you try to send messages to a producer
    # object that has been #shutdown
    class ProducerShutdownError < StandardError; end
  end

  def self.logger
    @logger ||= null_logger
  end

  def self.logger=(logger)
    @logger = logger
  end

  private
  def self.null_logger
    devnull = RUBY_PLATFORM =~ /w32/ ? 'nul' : '/dev/null'
    l = Logger.new(devnull)
    l.level = Logger::INFO
    l
  end
end

# Public API
require "poseidon/message_to_send"
require "poseidon/producer"
require "poseidon/fetched_message"
require "poseidon/partition_consumer"
require "poseidon/new_producer"

# Poseidon!
require "poseidon/message"
require "poseidon/message_set"
require "poseidon/topic_metadata"
require "poseidon/protocol"

require "poseidon/broker_pool"
require "poseidon/cluster_metadata"
require "poseidon/compression"
require "poseidon/connection"
require "poseidon/message_conductor"
require "poseidon/messages_for_broker"
require "poseidon/messages_to_send"
require "poseidon/messages_to_send_batch"
require "poseidon/producer_compression_config"
require "poseidon/sync_producer"
require "poseidon/version"

require "poseidon/node"
require "poseidon/record_accumulator"
require "poseidon/producer_sender"
require "poseidon/selector"
require "poseidon/network_client"
require "poseidon/stream"
require "poseidon/topic_partition"
require "poseidon/client_request"
require "poseidon/request_send"
require "poseidon/network_send"
require "poseidon/network_receive"
require "poseidon/in_flight_requests"
require "poseidon/client_response"
require "poseidon/record_batch"
require "poseidon/future_record_metadata"
require "poseidon/produce_request_result"
require "poseidon/record_metadata"
require "poseidon/ready_check_result"
require "poseidon/record_append_result"
require "poseidon/memory_records"
#require "poseidon/java_producer"
