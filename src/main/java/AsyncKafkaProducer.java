import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AsyncKafkaProducer {
 public static void main(String... args) throws Exception {
  doRunProducer(5);

 }

 static void doRunProducer(final int sendMessageCount) throws InterruptedException {
  String topicName = "topic-test-1";
  Properties props = new Properties();
  props.put("bootstrap.servers", "localhost:9092");
  props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

  long time = System.currentTimeMillis();

  Producer<Long, String> producer = new KafkaProducer<Long, String>(props);
  final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

  try {
   for (long index = time; index < time + sendMessageCount; index++) {
    final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topicName, index,
      "Hello message " + index);
    producer.send(record, (metadata, exception) -> {
     long elapsedTime = System.currentTimeMillis() - time;
     if (metadata != null) {
      System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
        record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
     } else {
      exception.printStackTrace();
     }
     countDownLatch.countDown();
    });
   }
   countDownLatch.await(25, TimeUnit.SECONDS);
  } finally {
   producer.flush();
   producer.close();
  }
 }

}