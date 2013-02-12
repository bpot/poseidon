$:.unshift File.expand_path(File.dirname(__FILE__) + '/../lib')
require 'poseidon'

producer = Poseidon::PartitionConsumer.new("example_consumer", "localhost", 9092,
                                           "example", 0, :earliest_offset)

loop do
  begin
    messages = producer.fetch
    messages.each do |m|
      puts "Received message: #{m.value}"
    end
  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic does not exist yet"
  end

  sleep 1
end
