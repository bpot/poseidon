require 'integration/multiple_brokers/spec_helper'

describe "handling failures" do
  describe "metadata failures" do
    before(:each) do
      @messages_to_send = [
        MessageToSend.new("topic1", "hello"),
        MessageToSend.new("topic2", "hello")
      ]
    end

=begin
    describe "unable to connect to brokers" do
      before(:each) do
        @p = Producer.new(["localhost:1092","localhost:1093","localhost:1094"], "producer")
      end

      it "triggers callback failures for both topics" do
        callback_triggered = []
        @p.send_messages(@messages_to_send) do |result|
          callback_triggered << result.topic

          expect(result.success?).to eq(false)
          expect(result.messages.size).to eq(1)
          expect(result.error).to eq(Poseidon::Errors::UnableToFetchMetadata)
        end
        expect(callback_triggered.sort).to eq(["topic1", "topic2"])
      end
    end
=end

    describe "only one of two topics exists" do
      before(:each) do
        @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "producer", :max_send_retries => 0)

        c = Connection.new("localhost", 9092, "metadata_fetcher")
        md = c.topic_metadata(["topic1"])
        sleep 5
        md = c.topic_metadata(["topic1"])
        pp md
      end

      # XXX sister one where this succeeds because we have retries!
      # XXX sister one where required_acks > 1
      it "triggers one callback failure and one success" do
        success_triggered = 0
        failure_triggered = 0

        @p.send_messages(@messages_to_send) do |result|
          p "CALLBACK YO"

          if result.success?
            p "GOT SUCCESS"
            success_triggered += 1 
            expect(result.messages.size).to eq(1)
            expect(result.error).to eq(nil)
            expect(result.topic).to eq("topic1")
          else
            p "GOT FAILURE"
            failure_triggered += 1 
            expect(result.messages.size).to eq(1)
            # XXX should this be unknown topic?!
            expect(result.error).to eq(Poseidon::Errors::UnableToFetchMetadata)
            expect(result.topic).to eq("topic2")
          end
        end

        expect(success_triggered).to eq(1)
        expect(failure_triggered).to eq(1)
      end
    end
  end
=begin
  describe "unknown topic" do
    it "receives error callback" do
      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "imnothere")

      @p.send_messages([MessagesToSend.new("imnothere", "hello")]) do |result|
        expect(result.success?).to eq(false)
        expect(result.error).to eq(
      end
    end
  end
=end

=begin
  describe "broker fails" do
    it "refreshes metadata and writes to the other broker" do
      c = Connection.new("localhost", 9092, "metadata_fetcher")
      md = c.topic_metadata(["test"])
      sleep 1
      md = c.topic_metadata(["test"])

      test_topic = md.topics.first

      consumers = test_topic.partitions.map do |partition|
        leader_id = partition.leader
        broker = md.brokers.find { |b| b.id == leader_id }
        PartitionConsumer.new("test_consumer_#{partition.id}", broker.host,
                              broker.port, "test", partition.id, -1)
      end

      # Update offsets to current position before adding test messages
      consumers.each do |c|
        c.fetch
      end

      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "test",
                       :required_acks => 1)

      24.times do
        @p.send_messages([MessageToSend.new("test", "hello")])
      end

      consumers.each do |c|
        messages = c.fetch
        expect(messages.size).to eq(8)
      end


      # Again with one less broker
      $tc.broker.without_process do
        @p.send_messages([MessageToSend.new("test", "hello")])
        expect(@p.send_messages([MessageToSend.new("test", "hello")])).to eq(false)
      end


    end
  end
=end
end
