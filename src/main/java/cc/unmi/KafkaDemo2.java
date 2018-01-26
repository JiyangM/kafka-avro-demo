package cc.unmi;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Collections;
import java.util.Properties;


/**
 * 参考：https://www.iteblog.com/archives/2210.html
 */
public class KafkaDemo2 {

    /**
     * 生产者传送avro编码
     */
    public static class AvroProducer {

        public static final String USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"Iteblog\","
                + "\"fields\":["
                + "  { \"name\":\"str1\", \"type\":\"string\" },"
                + "  { \"name\":\"str2\", \"type\":\"string\" },"
                + "  { \"name\":\"int1\", \"type\":\"int\" }"
                + "]}";

        public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put("bootstrap.servers", "www.iteblog.com:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(USER_SCHEMA);

            // 使用twitter序列化工具
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

            for (int i = 0; i < 1000; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("str1", "str 1-" + i);
                avroRecord.put("str2", "str 2-" + i);
                avroRecord.put("int1", i);

                byte[] bytes = recordInjection.apply(avroRecord);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic", bytes);
                producer.send(record);
                Thread.sleep(260);
            }
            producer.close();
        }
    }


    /**
     * 消费者 读取avro 数据
     */
//    public static class AvroKafkaConsumer {
//        private final ConsumerCo consumer;
//        private final String topic;
//
//        public AvroKafkaConsumer(String zookeeper, String groupId, String topic) {
//            Properties props = new Properties();
//            props.put("zookeeper.connect", zookeeper);
//            props.put("group.id", groupId);
//            props.put("zookeeper.session.timeout.ms", "500");
//            props.put("zookeeper.sync.time.ms", "250");
//            props.put("auto.commit.interval.ms", "1000");
//
//            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
//            this.topic = topic;
//        }
//
//        public void testConsumer() {
//            Map<String, Integer> topicCount = new HashMap<>();
//            topicCount.put(topic, 1);
//
//            Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
//            List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
//            for (final KafkaStream stream : streams) {
//                ConsumerIterator it = stream.iterator();
//                while (it.hasNext()) {
//                    MessageAndMetadata messageAndMetadata = it.next();
//                    String key = new String((byte[])messageAndMetadata.key());
//                    byte[] message = (byte[]) messageAndMetadata.message();
//
//                    Schema.Parser parser = new Schema.Parser();
//                    Schema schema = parser.parse(AvroKafkaProducter.USER_SCHEMA);
//                    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
//                    GenericRecord record = recordInjection.invert(message).get();
//                    logger.info("key=" + key + ", str1= " + record.get("str1")
//                            + ", str2= " + record.get("str2")
//                            + ", int1=" + record.get("int1"));
//                }
//            }
//            consumer.shutdown();
//        }
//
//        public static void main(String[] args) {
//            AvroKafkaConsumer simpleConsumer =
//                    new AvroKafkaConsumer("www.iteblog.com:2181", "testgroup", "iteblog");
//            simpleConsumer.testConsumer();
//        }
//    }

    /**
     * 0.9 版本实现
     */
    public class AvroKafkaConsumer09 {
        public static void main(String[] args) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "www.iteblog.com:2181");
            props.put("group.id", "testgroup");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            String topic = "iteblog";

            consumer.subscribe(Collections.singletonList(topic));
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(AvroProducer.USER_SCHEMA);
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            try {
                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        GenericRecord genericRecord = recordInjection.invert(record.value()).get();
                        System.out.println("topic = %s, partition = %s, offset = %d, customer = %s,country = %s\n", record.topic(), record.partition(), record.offset(), " +
                                record.key(), genericRecord.get("str1"));
                    }
                }
            } finally {
                consumer.close();
            }
        }
    }
}
