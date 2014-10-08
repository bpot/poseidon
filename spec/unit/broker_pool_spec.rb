require 'spec_helper'

describe BrokerPool do
  context "empty broker list" do
    it "raises UnknownBroker error when trying to produce data" do
      expect { BrokerPool.new("test_client", [], 10_000).execute_api_call(0, :produce) }.to raise_error(BrokerPool::UnknownBroker)
    end
  end

  describe "fetching metadata" do
    context "single broker" do
      it "initializes connection properly" do
        @broker_pool = BrokerPool.new("test_client", ["localhost:9092"], 2_000)
        @broker = double('Poseidon::Connection', :topic_metadata => nil)

        expected_args = ["localhost", "9092", "test_client", 2_000]
        connection = double('conn').as_null_object

        expect(Connection).to receive(:new).with(*expected_args).and_return(connection)

        @broker_pool.fetch_metadata(Set.new)
      end
    end

    context "no seed brokers" do
      it "raises Error" do
        @broker_pool = BrokerPool.new("test_client", [], 10_000)
        expect { @broker_pool.fetch_metadata(Set.new) }.to raise_error(Errors::UnableToFetchMetadata)
      end
    end

    context "2 seed brokers" do
      before(:each) do
        @broker_pool = BrokerPool.new("test_client", ["first:9092","second:9092"], 10_000)
        @broker_1 = double('Poseidon::Connection_1', :topic_metadata => nil, :close => nil)
        @broker_2 = double('Poseidon::Connection_2', :topic_metadata => double('topic_metadata').as_null_object, :close => nil)
        allow(Connection).to receive(:new).and_return(@broker_1, @broker_2)
      end

      context ", first doesn't have metadata" do
        it "asks the second" do
          expect(@broker_2).to receive(:topic_metadata)

          @broker_pool.fetch_metadata(Set.new)
        end
      end

      it "cleans up its connections" do
        expect(@broker_1).to receive(:close)
        expect(@broker_2).to receive(:close)

        @broker_pool.fetch_metadata(Set.new)
      end
    end
  end

  context "which knowns about two brokers" do
    before(:each) do
      @broker_pool = BrokerPool.new("test_client", [], 10_000)
      @broker_pool.update_known_brokers({0 => { :host => "localhost", :port => 9092 }, 1 => {:host => "localhost", :port => 9093 }})
    end

    describe "when executing a call" do

      it "creates a connection for the correct broker" do
        c = double('conn').as_null_object
        expected_args = ["localhost", 9092, "test_client", 10_000]

        expect(Connection).to receive(:new).with(*expected_args).and_return(c)
        @broker_pool.execute_api_call(0, :produce)
      end

      it "it does so on the correct broker" do
        c = double('conn').as_null_object
        allow(Connection).to receive(:new).and_return(c)

        expect(c).to receive(:produce)
        @broker_pool.execute_api_call(0, :produce)
      end
    end

    describe "when executing two calls" do
      it "reuses the connection" do
        c = double('conn').as_null_object

        expect(Connection).to receive(:new).once.and_return(c)
        @broker_pool.execute_api_call(0, :produce)
        @broker_pool.execute_api_call(0, :produce)
      end
    end

    describe "executing a call for an unknown broker" do
      it "raises UnknownBroker" do
        expect { @broker_pool.execute_api_call(2, :produce) }.to raise_error(BrokerPool::UnknownBroker)
      end
    end
  end
end
