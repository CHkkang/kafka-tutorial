package chan.hyeok.kang.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 특정 오프셋 읽어오기 (왜 안나옴?)
 */
public class ReadRecordMain {
    private static final String TOPIC = "customerCountries";
    Duration timeout = Duration.ofMillis(100);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        long oneHourEarlier = Instant.now().atZone(ZoneId.systemDefault()).minusMinutes(10).toEpochSecond();
        // assignment -> 현재 Kafka Consumer가 할당받은 TopicPartition들의 목록을 반환
        Map<TopicPartition, Long> partitionTimestampMap = consumer.assignment()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));

        System.out.println(partitionTimestampMap);

        // 브로커에 요청을 보내서 타임스탬프 인덱스에 저장된 오프셋 리턴
        // 즉 파티션에서 oneHourEarlier전에 기록된 타임스탬프에 해당하는 메시지가 저장된 오프셋을 반환합니다.
        Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(partitionTimestampMap);

        for (Map.Entry<TopicPartition,OffsetAndTimestamp> entry: offsetMap.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue().offset());
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}
