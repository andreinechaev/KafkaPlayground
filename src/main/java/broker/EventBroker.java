package broker;

import com.sun.istack.internal.NotNull;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import settings.ServiceHelper;
import settings.Settings;
import zk.ZooKeeperStartable;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
 */
public class EventBroker {

    private ZooKeeperStartable mZKServer;
    private KafkaServerStartable mKafkaServer;

    public EventBroker(@NotNull Map<String, String> properties) throws Exception {
        String zkProps = properties.get(Settings.ZOOKEEPER);
        Properties zp = ServiceHelper.loadProperties(zkProps);

        mZKServer = new ZooKeeperStartable(zp);

        String kafkaProps = properties.get(Settings.KAFKA);
        Properties kp = ServiceHelper.loadProperties(kafkaProps);
        KafkaConfig kafkaConfig = new KafkaConfig(kp);

        mKafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public void createTopic(String name, int partition, int replication) {
        mZKServer.createTopic(name, partition, replication);
    }

    public void start() throws Exception {
        if (mZKServer == null || mKafkaServer == null) {
            throw new Exception("Could not load the Broker");
        }

        mZKServer.start();
        mKafkaServer.startup();
        System.out.println("Servers started");
    }

    public void stop() throws Exception {
        if (mKafkaServer == null) {
            throw new Exception("Could not stop the Broker");
        }
        mKafkaServer.shutdown();
        mZKServer.stop();

        if (mZKServer.isRunning()) {
            System.out.println("still running");
        }
    }

}
