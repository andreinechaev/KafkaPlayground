import broker.EventBroker;
import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import producer.Producer;
import settings.ServiceHelper;
import settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
 */

public class Main {
    private static final String TOPIC = "message";

    private static ExecutorService service = Executors.newSingleThreadExecutor();
    private static ExecutorService es = Executors.newFixedThreadPool(2);

    public static void main(String[] args) throws Exception {
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put(Settings.KAFKA, "kafka.props");
        brokerProps.put(Settings.ZOOKEEPER, "zk.props");

        EventBroker broker = new EventBroker(brokerProps);
        service.execute(() -> {
            try {
                broker.start();
                broker.createTopic(TOPIC, 1, 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        service.shutdown();

        Thread.sleep(10_000);

        Runnable pTask = () -> {
            System.out.println("pTask");
            try {
                Producer<String, String> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
                for (int i = 0; i < 2000; i++) {
                    ProducerRecord<String, String> pr = new ProducerRecord<>(TOPIC, "Msg", "send " + String.valueOf(i));
                    producer.send(pr);
                    producer.flush();
                }

                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        Runnable cTask = () -> {
            System.out.println("cTask");
            try {
                Consumer<String, String> consumer = new Consumer<>(ServiceHelper.loadProperties("consumer.props"));
                consumer.subscribe(Collections.singletonList(TOPIC));
                ConsumerRecords<String, String> crs = consumer.poll(2000);
                System.out.println("Records count = " + crs.count());
//                for (ConsumerRecord<String, String> cr : crs) {
//                    System.out.println(cr.key() + ": " + cr.value());
//                }

                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);
        es.execute(pTask);

        es.shutdown();
        System.out.println("Waiting es");
        es.awaitTermination(60, TimeUnit.MINUTES);

        try {
            broker.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Waiting service to stop");
        service.awaitTermination(60, TimeUnit.MINUTES);
        System.out.println("End of execution");
    }
}
