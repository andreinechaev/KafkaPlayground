import broker.EventBroker;
import consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;
import producer.Producer;
import settings.ServiceHelper;
import settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by anechaev on 7/25/16.
 * © Andrei Nechaev 2016
 */

public class Main {
    private static final String TOPIC = "message";

    private static ExecutorService es = Executors.newFixedThreadPool(2);

    public static void main(String[] args) throws Exception {
//        BasicConfigurator.configure();

        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put(Settings.KAFKA, "kafka.props");
        brokerProps.put(Settings.ZOOKEEPER, "zk.props");

        EventBroker broker = new EventBroker(brokerProps);


        broker.start();
        broker.createTopic(TOPIC, 1, 1);

        Runnable pTask = () -> {
            System.out.println("pTask");
            try {
                Producer<String, String> producer = new Producer<>(ServiceHelper.loadProperties("producer.props"));
                for (int i = 0; i < 100; i++) {
                    Thread.sleep(100);
                    ProducerRecord<String, String> pr = new ProducerRecord<>(TOPIC, "Msg", "send " + String.valueOf(i));
                    producer.send(pr);
                    producer.flush();
                }

                producer.close();
            } catch (IOException | InterruptedException e) {
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
                while (crs.iterator().hasNext()) {
                    ConsumerRecord<String, String> record = crs.iterator().next();
                    System.out.println(record.key() + ": " + record.value());
                    crs = consumer.poll(2000);
                }

                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        es.execute(cTask);
        Thread.sleep(1000);
        es.execute(pTask);

        es.shutdown();

        System.out.println("Waiting es");
        es.awaitTermination(60, TimeUnit.MINUTES);

        try {
            broker.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("End of execution");
    }
}
