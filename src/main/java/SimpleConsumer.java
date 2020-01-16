import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class SimpleConsumer {
 public static void main(String[] args) throws Exception {
  String topicName = "topic-test-1";
  String groupName = "mygroup";
  Properties props = new Properties();
  props.put("bootstrap.servers", "localhost:9092");
  props.put("group.id", groupName);
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  
  KafkaConsumer<String, String> consumer = null;
  try {
   consumer = new KafkaConsumer<String, String>(props);
   consumer.subscribe(Arrays.asList(topicName));
   while (true) {
    ConsumerRecords<String, String> records = consumer.poll(100);
    for (ConsumerRecord<String, String> record : records) {
     System.out.printf("Message received -> partition = %d, offset = %d, key = %s, value = %s\n",
       record.partition(), record.offset(), record.key(), record.value());
    }
   }
  } catch (Exception ex) {
   ex.printStackTrace();
  } finally {
   consumer.close();
  }
 }
}