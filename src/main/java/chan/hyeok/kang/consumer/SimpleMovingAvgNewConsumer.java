package chan.hyeok.kang.consumer;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;

public class SimpleMovingAvgNewConsumer {
    private Properties kafkaProps = new Properties();
    private String waitTime;
    private KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        final SimpleMovingAvgNewConsumer movingAvg = new SimpleMovingAvgNewConsumer();
        String brokers = "localhost:9092";
        String groupId = "CountryCounter";
        String topic = "customerCountries";
        int window = 3;

        CircularFifoBuffer buffer = new CircularFifoBuffer(window);
        movingAvg.configure(brokers, groupId);

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // 컨슈머에게 종료 신호를 보내고, 소비자가 현재 poll() 호출에서 대기하고 있더라도 즉시 반환
                // 컨슈머가 더 이상 메시지를 읽지 않도록 하기 위해
                movingAvg.consumer.wakeup();

                try {
                    mainThread.join(); // 메인스레드 종료될떄까지 대기
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Duration timeout = Duration.ofMillis(1000);
        try {
            movingAvg.consumer.subscribe(Collections.singletonList(topic));

            // looping until ctrl-c, the shutdown hook will cleanup on exit
            while (true) {
                ConsumerRecords<String, String> records = movingAvg.consumer.poll(timeout);
                System.out.println(Instant.now().atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

                    int sum = 0;

                    try {
                        int num = Integer.parseInt(record.value());
                        buffer.add(num);
                    } catch (NumberFormatException e) {
                        // just ignore strings
                    }

                    for (Object o : buffer) {
                        sum += (Integer) o;
                    }

                    if (buffer.size() > 0) {
                        System.out.println("Moving avg is: " + (sum / buffer.size()));
                    }
                }
                for (TopicPartition tp: movingAvg.consumer.assignment())
                    System.out.println("Committing offset at position:" + movingAvg.consumer.position(tp));
                movingAvg.consumer.commitSync();
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            movingAvg.consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }

    private void configure(String servers, String groupId) {
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");         // when in doubt, read everything
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(kafkaProps);
    }

}