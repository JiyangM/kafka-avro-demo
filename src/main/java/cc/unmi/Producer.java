package cc.unmi;

import cc.unmi.serialization.AvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;



public class Producer<T extends SpecificRecordBase> {

    private KafkaProducer<String, T> producer = new KafkaProducer<>(getProperties());

    public void sendData(Topic topic, T data) {
        producer.send(new ProducerRecord<>(topic.topicName, data),
                (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent user: %s \n", data);
                    } else {
                        System.out.println("data sent failed: " + exception.getMessage());
                    }
                });
    }

    private Properties getProperties() {
        Properties props = new Properties();
        // 建立kafka集群连接 格式为host1:port1,host2:port2,
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 当向server发出请求时，这个字符串会发送给server。用来追踪数据的生产者
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        return props;
    }
}
