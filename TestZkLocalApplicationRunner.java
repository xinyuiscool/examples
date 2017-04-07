package org.apache.samza.test.integration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.zk.ZkCoordinationServiceFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;


public class TestZkLocalApplicationRunner {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(TestZkLocalApplicationRunner.class);

  private static final String ZK_CONNECT_DEFAULT = "localhost:2182";
  private static final String ZK_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";

  private Map<String, String> createConfigs() {
    Map<String, String> configs = new HashMap<>();

    // create internal system
    configs.put("systems.queuing.samza.factory","org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.queuing.producer.bootstrap.servers","ltx1-kafka-kafka-queuing-vip.stg.linkedin.com:10251");
    configs.put("systems.queuing.consumer.zookeeper.connect", "zk-ltx1-kafka.stg.linkedin.com:12913/kafka-queuing");
    configs.put("systems.queuing.samza.key.serde", "string");
    configs.put("systems.queuing.samza.msg.serde", "string");
    configs.put("systems.queuing.samza.offset.default", "oldest");
    configs.put("job.default.system", "queuing");

    // serde
    configs.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

    // streams
    configs.put("streams.samza-perf-test-48.samza.system", "queuing");
    configs.put("streams.XiliuPageViewEvent333.samza.system", "queuing");

    // zk
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, ZK_JOB_COORDINATOR_FACTORY);
    configs.put(ZkConfig.ZK_CONNECT, ZK_CONNECT_DEFAULT);

    // jobs
    configs.put("task.name.grouper.factory",
        "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put("job.name", "test-app");
    configs.put("job.id", "i001");

    // app
    configs.put(ApplicationConfig.APP_COORDINATION_SERVICE_FACTORY_CLASS, ZkCoordinationServiceFactory.class.getName());

    return configs;
  }

  public static void main(String[] args) throws Exception {

    TestZkLocalApplicationRunner test = new TestZkLocalApplicationRunner();



    startZookeeper();

    //clean up
    ZkClient zkClient = new ZkClient(ZK_CONNECT_DEFAULT);
    zkClient.deleteRecursive("/test-app-i001");
    zkClient.close();
    ///////////////////////////////

    StreamApplication streamApp = new StreamApplication() {
      @Override
      public void init(StreamGraph graph, Config config) {
        graph.getInputStream("samza-perf-test-48", (k, v) -> (String) v)
            .partitionBy(m -> String.valueOf(m.hashCode() % 10)).map(m -> m)
            .sendTo(graph.getOutputStream("XiliuPageViewEvent333", m -> "hahaha", m -> m));
      }
    };

    LOG.info("start runner!");

    try {
      Map<String, String> config = new HashMap<>(test.createConfigs());
      config.put("processor.id", "0");
      LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));
      runner.run(streamApp);

      Thread.sleep(600000);

      LOG.info("kill runner!");
      runner.kill(streamApp);

      Thread.sleep(100000000);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public static Thread startZookeeper() {
    Properties startupProperties = new Properties();
    startupProperties.put("tickTime", "2000");
    // The number of ticks that the initial
    //synchronization phase can take
    startupProperties.put("initLimit", "10");
    // The number of ticks that can pass between
    // sending a request and getting an acknowledgement
    startupProperties.put("syncLimit", "5");
    //the directory where the snapshot is stored.
    startupProperties.put("dataDir", "/tmp/zookeeper");
    //the port at which the clients will connect
    startupProperties.put("clientPort","2182");


    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(startupProperties);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

    ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);

    Thread t = new Thread() {
      public void run() {
        try {
          zooKeeperServer.runFromConfig(configuration);
        } catch (IOException e) {
          LOG.error("ZooKeeper Failed", e);
        }
      }
    };
    t.setDaemon(true);
    t.start();

    return t;
  }
}
