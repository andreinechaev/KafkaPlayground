package broker;

import com.sun.istack.internal.NotNull;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import settings.ServiceHelper;
import settings.Settings;
import zk.ZooKeeperStartable;

import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
 */
public class EventBroker {
    private final ExecutorService mZKService = Executors.newSingleThreadExecutor();
    private final ExecutorService mInternalService = Executors.newCachedThreadPool();

    private ZooKeeperStartable mZKServer;
    private KafkaServerStartable mKafkaServer;

    public EventBroker(@NotNull Map<String, String> properties) throws Exception {
        String zkProps = properties.get(Settings.ZOOKEEPER);
        Properties zp = ServiceHelper.loadProperties(zkProps);
        zp.setProperty("dataDir", Files.createTempDirectory("zp-").toAbsolutePath().toString());
        mZKServer = new ZooKeeperStartable(zp);

        String kafkaProps = properties.get(Settings.KAFKA);
        Properties kp = ServiceHelper.loadProperties(kafkaProps);
        kp.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        KafkaConfig kafkaConfig = new KafkaConfig(kp);

        mKafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public void start() throws Exception {
        if (mZKServer == null || mKafkaServer == null) {
            throw new Exception("Could not load the Broker");
        }

        mZKService.execute(mZKServer.start());
        mZKService.shutdown();

        String hostAddress = mZKServer.getHostAddress();

        ZkClient client = new ZkClient(
                hostAddress,
                Settings.CONNECTION_TIMEOUT,
                Settings.SESSION_TIMEOUT,
                ZKStringSerializer$.MODULE$);

        ZkUtils utils = ZkUtils.apply(client, false);
        mZKServer.setUtils(utils);

        mKafkaServer.startup();
        System.out.println("Broker started");
    }


    public void stop() throws Exception {
        if (mKafkaServer == null) {
            throw new Exception("Could not stop the Broker");
        }
        mInternalService.shutdown();
        mKafkaServer.shutdown();
        mZKServer.stop();

        mInternalService.awaitTermination(8, TimeUnit.SECONDS);
        mZKService.awaitTermination(60, TimeUnit.SECONDS);
    }

    public void createTopic(String name, int partition, int replication) {
        mInternalService.execute(() -> mZKServer.createTopic(name, partition, replication));
    }
}
