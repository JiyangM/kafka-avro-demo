package cc.unmi;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;



public class KafkaDemo2 {

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

            // 使用twitter开园2序列化工具
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

}
