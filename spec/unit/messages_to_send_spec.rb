require 'spec_helper'

RSpec.describe MessagesToSend do
  before(:each) do
    @messages = []
    @messages << Message.new(:topic => "test1", :value => "hi")
    @messages << Message.new(:topic => "test2", :value => "hi")
    @messages << Message.new(:topic => "test2", :value => "hi")


    @cluster_metadata = double('cluster_metdata').as_null_object
    @mts = MessagesToSend.new(@messages, @cluster_metadata)
  end

  describe "needing metadata" do
    it "returns set of topics" do
      expect(@mts.topic_set).to eq(Set.new(["test1","test2"]))
    end

    it "asks ClusterMetadata about having metadata" do
      allow(@cluster_metadata).to receive(:have_metadata_for_topics?).and_return(true)

      expect(@mts.needs_metadata?).to eq(false)
    end
  end

  describe "sending" do
    before(:each) do
      @mfb = double('mfb', :messages => @messages)
      @messages_for_brokers = [@mfb]

      @mtsb = double('messages_to_send_batch').as_null_object
      allow(@mtsb).to receive(:messages_for_brokers).and_return(@messages_for_brokers)

      allow(MessagesToSendBatch).to receive(:new).and_return(@mtsb)
    end

    context "is successful" do
      before(:each) do
        @mts.messages_for_brokers(nil).each do |mfb|
          @mts.successfully_sent(mfb.messages)
        end
      end

      it "successfully sends all" do
        expect(@mts.pending_messages?).to eq(false)
      end
    end

    context "is not successful" do
      before(:each) do
        @mts.messages_for_brokers(nil).each do |mfb|
        end
      end

      it "does not send all" do
        @mts.messages_for_brokers(nil).each do |mfb|
        end
        expect(@mts.pending_messages?).to eq(true)
      end
    end
  end
end
