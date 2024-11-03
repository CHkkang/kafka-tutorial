package chan.hyeok.kang.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 2. 오프셋 수동 커밋
 */
public class ManualCommitMain {
    private Properties kafkaProps = new Properties();
    private static final Map<String, Integer> customCountryMap = new HashMap<>();
    private static final String TOPIC = "customerCountries";

    // 하나의 스레드당 하나의 컨슈머
    public static void main(String[] args) {
        Properties props = new Properties();
        // 첫 연결을 생성하기 위한 브로커의 host 목록
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 자동 오프셋 커밋
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 수동 오프셋 커밋
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribing to the topic 'customerCountries'
        consumer.subscribe(Collections.singletonList(TOPIC));

        Duration timeout = Duration.ofMillis(100);

        /**
         * 오프셋 커밋 방법 예제
         */
        // 1. 자동 오프셋 커밋 (basic) ENABLE_AUTO_COMMIT_CONFIG = true
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(timeout);
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
//                            record.topic(), record.partition(), record.offset(),
//                            record.key(), record.value());
//
//                    // Update country count
//                    int updatedCount = customCountryMap.getOrDefault(record.value(), 0) + 1;
//                    customCountryMap.put(record.value(), updatedCount);
//
//                    // Print the country map in JSON format
//                    JSONObject json = new JSONObject(customCountryMap);
//                    System.out.println(json.toString());
//                }
//            }
//        } finally {
//            consumer.close();
//        }

        // 2. 동기 오프셋 커밋 commitSync() example ENABLE_AUTO_COMMIT_CONFIG = false
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            try {
//                consumer.commitAsync(); // 처리에 대한 완료되면 추가 메시지를 폴링하기 전 마지막 오프셋 커밋
            } catch (CommitFailedException e) {
                // 처리할 수 없는 에러가 발생할 경우 에러를 로깅하는 것 밖에 할 수 있는 게 없음..
                System.out.println("Failed to commit : " + e.toString());
            }
        }

        // 3. 비동기 오프셋 커밋 : commitAsync() example ENABLE_AUTO_COMMIT_CONFIG = false
        // ConsumerRecord 처리 순서를 보장하지 못함
        // 재시도가 가능한 실패가 발생할 경우에도 재시도 하지 않음
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(timeout);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
//                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
//            }
//
//            consumer.commitAsync();
//        }

        // 4. 비동기 오프셋 커밋 : commitAsync() example ENABLE_AUTO_COMMIT_CONFIG = false
        // 콜백으로 로깅을 잡는게 일반적이나, 재시도를 위해 콜백을 사용할 경우에는 커밋 순서 주의를 해야함
        // 순차적으로 단조 증가하는 번호를 사용하면 비동기적 커밋 재시도 시 오프셋 순서를 맞출수 있음 -> 흠
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(timeout);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
//                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
//            }
//
//            consumer.commitAsync(new OffsetCommitCallback() {
//                @Override
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                    if (e != null) {
//                        System.out.println("Failed to commit offsets");
//                    }
//                }
//            });
//        }

        // 5. 동기 커밋 + 비동기 커밋
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(timeout);
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
//                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
//                }
//                consumer.commitAsync();
//            }
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        } finally {
//            try {
//                // 컨슈머를 닫기 전 혹은 rebalance 전 마지막 커밋이라면, 성공 여부를 확인해야함!
//                consumer.commitSync();
//            } finally {
//                consumer.close();
//            }
//        }

        // 6. 특정 오프셋 커밋하기
//        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
//        int count = 0;
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(timeout);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
//                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
//
//
//                currentOffsets.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, "no metadata (nullable)")
//                );
//
//                // 1000개 레코드마다 현재 오프셋 커밋
//                // 실제 application 에서는 시간 혹은 레코드 내용물 기준으로 커밋하는 것이 좋음
//                if (count % 1000 == 0) {
//                    consumer.commitAsync(currentOffsets, null);
//                    //consumer.commitSync(currentOffsets);
//                }
//
//                count++;
//            }
//        }
    }
}