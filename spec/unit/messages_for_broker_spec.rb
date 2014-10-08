require 'spec_helper'

RSpec.describe MessagesForBroker do
  context "twos message one to broker 0, partition 0, another to partition 1" do
    before(:each) do
      @messages = [ Message.new(:topic => "topic1",:value => "hi0"),
                    Message.new(:topic => "topic1",:value => "hi1")]

      @compression_config = double('compression_config',
                                 :compression_codec_for_topic => nil)

      @mfb = MessagesForBroker.new(0)
      @mfb.add(@messages[0], 0)
      @mfb.add(@messages[1], 1)
    end

    it "provides the messages" do
      expect(@mfb.messages.to_set).to eq(@messages.to_set)
    end

    it "is has a broker_id of 0" do
      expect(@mfb.broker_id).to eq(0)
    end

    it "builds the protocol object correctly" do
      protocol_object = @mfb.build_protocol_objects(@compression_config)

      messages_for_topics = [
        MessagesForTopic.new("topic1",
          [
            MessagesForPartition.new(0, MessageSet.new([@messages[0]])),
            MessagesForPartition.new(1, MessageSet.new([@messages[1]])),
          ])
      ]
      expect(protocol_object).to eq(messages_for_topics)
    end

    context "and topic is compressed" do
      it "builds the protocol object correctly" do
        allow(@compression_config).to receive_messages(:compression_codec_for_topic => Compression::GzipCodec)
        protocol_object = @mfb.build_protocol_objects(@compression_config)

        messages_for_topics = [
          MessagesForTopic.new("topic1",
            [
              MessagesForPartition.new(0, MessageSet.new([@messages[0]]).compress(Compression::GzipCodec)),
              MessagesForPartition.new(1, MessageSet.new([@messages[1]]).compress(Compression::GzipCodec)),
            ])
        ]
        expect(protocol_object).to eq(messages_for_topics)
      end
    end
  end
end
