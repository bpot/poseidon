$:.unshift File.expand_path(File.dirname(__FILE__) + '/../lib')
require 'poseidon'

producer = Poseidon::Producer.new(["localhost:9092"], "example_producer")

loop do
  producer.send_messages([Poseidon::MessageToSend.new("example", Time.now.to_s)])
  sleep 1
end
