import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class SimpleProducer {
 public static void main(String[] args) throws InterruptedException, ExecutionException {
  String topicName = "topic-test-1";
  Properties props = new Properties();
  props.put("bootstrap.servers", "localhost:9092");
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("acks", "all");
  Producer<String, String> producer = new KafkaProducer<String, String>(props);
  for (int i = 0; i < 10; i++) {
   String key = "Key" + i;
   String message = "Message from topic-test-" + i;
   /* Asynchronously send a record to a topic and returns RecordMetadata */
   Future<RecordMetadata> out = producer.send(new ProducerRecord<String,
        String>(topicName, key, message));
   String messageOut = " Topic: "+ out.get().topic() + " "+ " Partition: "+ out.get().partition() +
     " "+ " Offset: "+out.get().offset() +  " Message: "+message;
   System.out.println(messageOut);
  }
  producer.close();
  System.out.println("Message sent successfully");

 }
}