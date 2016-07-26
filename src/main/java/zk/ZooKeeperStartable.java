package zk;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/25/16.
 * © Andre Nechaev 2016
 */
public class ZooKeeperStartable {
    private final ServerConfig mConfiguration = new ServerConfig();
    private ExecutorService mServiceExecutor = Executors.newSingleThreadExecutor();

    private ZooKeeperServerMain mZooKeeperServer;
    private ZkUtils mUtils;


    public ZooKeeperStartable(Properties properties) throws IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        mZooKeeperServer = new ZooKeeperServerMain();
        mConfiguration.readFrom(quorumConfiguration);
    }

    public void createTopic(String name, int partition, int replication) {
        AdminUtils.createTopic(mUtils, name, partition, replication, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    public void start() throws Exception {
        if (mZooKeeperServer == null) {
            throw new Exception("ZooKeeper failed");
        }

        Runnable task = () -> {
            System.out.println("Zookeeper started");
            try {
                mZooKeeperServer.runFromConfig(mConfiguration);
            } catch(Exception ie) {
                ie.printStackTrace();
            }
        };

        mServiceExecutor.execute(task);

        String serverAddress = mConfiguration.getClientPortAddress().getHostName()
                + ":"
                + mConfiguration.getClientPortAddress().getPort();

        System.out.println(serverAddress);
        ZkClient client = new ZkClient(serverAddress, 30_000, 30_000, ZKStringSerializer$.MODULE$);
        mUtils = ZkUtils.apply(client, false);
    }

    public void stop() throws InterruptedException {
        try {
            // Mm.. Don't do it unless you really need it
            Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(mZooKeeperServer);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        mServiceExecutor.shutdown();
        mServiceExecutor.awaitTermination(60, TimeUnit.MINUTES);
    }

    public boolean isRunning() {
        return !mServiceExecutor.isShutdown();
    }

}
