require 'spec_helper'

describe BrokerPool do
  context "empty broker list" do
    it "raises UnknownBroker error when trying to produce data" do
      expect { BrokerPool.new("test_client", []).execute_api_call(0, :produce) }.to raise_error(BrokerPool::UnknownBroker)
    end
  end

  describe "fetching metadata" do
    context "no seed brokers" do
      it "raises Error" do
        @broker_pool = BrokerPool.new("test_client", [])
        expect { @broker_pool.fetch_metadata(Set.new) }.to raise_error(Errors::UnableToFetchMetadata)
      end
    end

    context "2 seed brokers" do
      before(:each) do
        @broker_pool = BrokerPool.new("test_client", ["first:9092","second:9092"])
        @broker_1 = double('Poseidon::Connection_1', :topic_metadata => nil)
        @broker_2 = double('Poseidon::Connection_2', :topic_metadata => double('topic_metadata').as_null_object)
        Connection.stub!(:new).and_return(@broker_1, @broker_2)
      end

      context ", first doesn't have metadata" do
        it "asks the second" do
          @broker_2.should_receive(:topic_metadata)

          @broker_pool.fetch_metadata(Set.new)
        end
      end
    end
  end

  context "which knowns about two brokers" do
    before(:each) do
      @broker_pool = BrokerPool.new("test_client", [])
      @broker_pool.update_known_brokers({0 => { :host => "localhost", :port => 9092 }, 1 => {:host => "localhost", :port => 9093 }})
    end

    describe "when executing a call" do

      it "creates a connection for the correct broker" do
        c = stub('conn').as_null_object
        expected_args = ["localhost", 9092, "test_client"]

        Connection.should_receive(:new).with(*expected_args).and_return(c)
        @broker_pool.execute_api_call(0, :produce)
      end

      it "it does so on the correct broker" do
        c = stub('conn').as_null_object
        Connection.stub(:new).and_return(c)

        c.should_receive(:produce)
        @broker_pool.execute_api_call(0, :produce)
      end
    end

    describe "when executing two calls" do
      it "reuses the connection" do
        c = stub('conn').as_null_object

        Connection.should_receive(:new).once.and_return(c)
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
