package chan.hyeok.kang.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;

/**
 * 리밸런스
 */
public class RebalanceMain {
    private static final Map<String, Integer> customCountryMap = new HashMap<>();
    private static final String TOPIC = "customerCountries";
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    Duration timeout = Duration.ofMillis(100);

    private class HandleRebalance implements ConsumerRebalanceListener {
        private KafkaConsumer<String, String> consumer;

        public HandleRebalance(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        /**
         * 컨슈머에게 새로운 파티션이 할당되었을 때 호출
         * max.poll.timeout.ms 시간 안에 완료되어야함
         * when cooperative rebalance: rebalancing 발생 시 마다 호출
         * @param partitions
         */
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Handle partition assignment logic if needed
        }

        /**
         * 리밸런싱 중 컨슈머가 할당된 파티션을 잃게 되었을 때 호출
         * when cooperative rebalance: rebalancing가 완료되고 할당 해제될 파티션들만 호출 (오프셋 커밋해야 할당받는 컨슈머 시작지점을 알 수 있음)
         * @param partitions
         */
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Lost partitions in rebalance. Committing current offsets: " + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        /**
         * 컨슈머가 파티션을 잃었을 때 호출 (구현이 안되어있을경우 onPartitionsRevoked 호출)
         * when cooperative rebalance: 일반적인 상황에서는 호출 안됨 / 할당 해제 되기전 다른 컹슈머에 먼저 할당된 경우에만 호출됨
         * @param partitions
         */
        public void onPartitionsLost(Collection<TopicPartition> partitions) {

        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        RebalanceMain rebalanceMain = new RebalanceMain();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC), rebalanceMain.new HandleRebalance(consumer));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(rebalanceMain.timeout);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    rebalanceMain.currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null));
                }
                consumer.commitAsync(rebalanceMain.currentOffsets, null);
            }
        } catch (WakeupException e) {
            // ignore, we're closing
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync(rebalanceMain.currentOffsets);
            } finally {
                consumer.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }
}
