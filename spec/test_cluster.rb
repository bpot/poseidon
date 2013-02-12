require 'daemon_controller'

class TestCluster
  def self.remove_tmp
    FileUtils.rm_rf("#{POSEIDON_PATH}/tmp")
  end

  def initialize
    @zookeeper = ZookeeperRunner.new
    @broker = BrokerRunner.new(0, 9092)
  end

  def start
    @zookeeper.start
    @broker.start
  end

  def stop
    @zookeeper.stop
    @broker.stop
  end
end

class JavaRunner
  def self.kafka_path=(kafka_path)
    @kafka_path = kafka_path
  end

  def self.kafka_path
    @kafka_path
  end

  attr_reader :pid
  def initialize(id, start_cmd, port, properties = {})
    @id = id
    @properties = properties
    @pid = nil
    @start_cmd = start_cmd
    @port = port
  end

  def start
    write_properties
    #@pid = run
    run
  end

  def stop
    daemon_controller.stop
    #Process.kill(:TERM, @pid)
  end

  private

  def daemon_controller
    @dc ||= DaemonController.new(
      :identifier => @id,
      :start_command => "#{self.class.kafka_path}/#{@start_cmd} #{config_path} &>> #{log_path} & echo $! > #{pid_path}",
      :ping_command => [:tcp, '127.0.0.1', @port],
      :pid_file => pid_path,
      :log_file => log_path,
      :start_timeout => 25
    )
  end

  def run
    FileUtils.mkdir_p(log_dir)
    FileUtils.mkdir_p(pid_dir)
    daemon_controller.start
    #cmd = "#{self.class.kafka_path}/#{@start_cmd} #{config_path} &>> #{log_path} & echo pid:$! > #{pid_path}"
    #`#{cmd}`
    #IO.read(pid_path).split(":")[1].to_i
  end

  def write_properties
    FileUtils.mkdir_p(config_dir)
    File.open(config_path, "w+") do |f|
      @properties.each do |k,v|
        f.puts "#{k}=#{v}"
      end
    end
  end

  def pid_path
    "#{pid_dir}/#{@id}.pid"
  end

  def pid_dir
    "#{file_path}/pid"
  end

  def log_path
    "#{log_dir}/#{@id}.log"
  end

  def log_dir
    "#{file_path}/log"
  end

  def config_path
    "#{config_dir}/#{@id}.properties"
  end

  def config_dir
    "#{file_path}/config"
  end

  def file_path
    POSEIDON_PATH + "tmp/"
  end
end

class BrokerRunner
  DEFAULT_PROPERTIES = {
    "broker.id" => 0,
    "port" => 9092,
    "num.network.threads" => 2,
    "num.io.threads" => 2,
    "socket.send.buffer.bytes" => 1048576,
    "socket.receive.buffer.bytes" => 1048576,
    "socket.request.max.bytes" => 104857600,
    "log.dir" => "#{POSEIDON_PATH}/tmp/kafka-logs",
    "num.partitions" => 1,
    "log.flush.interval.messages" => 10000,
    "log.flush.interval.ms" => 1000,
    "log.retention.hours" => 168,
    "log.segment.bytes" => 536870912,
    "log.cleanup.interval.mins" => 1,
    "zk.connect" => "localhost:2181",
    "zk.connection.timeout.ms" => 1000000,
    "kafka.metrics.polling.interval.secs" => 5,
    "kafka.metrics.reporters" => "kafka.metrics.KafkaCSVMetricsReporter",
    "kafka.csv.metrics.dir" => "#{POSEIDON_PATH}/tmp/kafka_metrics",
    "kafka.csv.metrics.reporter.enabled" => "false",
  }

  def initialize(id, port)
    @id   = id
    @port = port
    @jr = JavaRunner.new("broker_#{id}", 
                         'bin/kafka-run-class.sh kafka.Kafka', 
                         port,
                         DEFAULT_PROPERTIES.merge(
                           "broker.id" => id,
                           "port" => port
                         ))
  end

  def pid
    @jr.pid
  end

  def start
    @jr.start
  end

  def stop
    @jr.stop
  end
end


class ZookeeperRunner
  def initialize
    @jr = JavaRunner.new("zookeeper", 'bin/zookeeper-server-start.sh', 
                         2181,
                         :dataDir => "#{POSEIDON_PATH}/tmp/zookeeper",
                         :clientPort => 2181,
                         :maxClientCnxns => 0)
  end

  def pid
    @jr.pid
  end
  
  def start
    @jr.start
  end

  def stop
    @jr.stop
  end
end
