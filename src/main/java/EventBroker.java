import com.google.common.io.Resources;
import com.sun.istack.internal.NotNull;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by anechaev on 7/25/16.
 */
public class EventBroker<K, V> {

    private KafkaProducer<K, V> mProducer;
    private KafkaConsumer<K, V> mConsumer;


    public EventBroker(@NotNull Map<String, String> properties) throws IOException {
        Properties producerProps = loadProps(properties.get("producer"));
        Properties consumerProps = loadProps(properties.get("consumer"));

        mProducer = new KafkaProducer<>(producerProps);
        mConsumer = new KafkaConsumer<>(consumerProps);
    }


    private Properties loadProps(String prop) throws IOException {

        InputStream is = Resources.getResource(prop).openStream();
        Properties props = new Properties();
        props.load(is);

        return props;
    }

}
