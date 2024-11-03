package chan.hyeok.kang.consumer;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.json.JSONObject;

/**
 * 1. 컨슈머 옵션
 */
public class OptionMain {
    private static final Map<String, Integer> customCountryMap = new HashMap<>();
    private static final String TOPIC = "customerCountries";

    // 하나의 스레드당 하나의 컨슈머
    public static void main(String[] args) {
        Properties props = new Properties();
        // 첫 연결을 생성하기 위한 브로커의 host 목록
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        /*
            FETCH_MIN_BYTES_CONFIG: 브로커로부터 레코드를 가져올 때 받는 데이터의 최소량을 지정
            - CPU 자원을 많이 사용하거나 컨슈머 수가 많을 때 브로커의 부하를 줄이기 위해 기본값보다 높게 설정 가능
            - 이 값을 증가시킬 경우 처리량이 적은 상황에서 지연이 발생하거나 처리량이 증가할 수 있음
            - 기본값: 1
         */
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        /*
            FETCH_MAX_WAIT_MS_CONFIG: 브로커가 요청에 응답하기 전 대기하는 최대 시간
            default : 500ms
         */
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        /*
            컨슈머가 그룹 코디네이터에게 하트비트를 보내지 않은 채로 session 이 지나가면 컨슈머가 죽은 것으로 판단
            대체로 session_time = 3 * heartbeat_interval_time 으로 잡음
         */
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        /*
            AUTO_OFFSET_RESET_CONFIG: 오프셋을 커밋한적 없거나, 오프셋이 유효하지 않을 때 파티션을 읽기 시작할때 작동을 정의
            - latest(default) : 유효한 오프셋이 없을 경우 가장 최근 레코드
            - earliest : 유효한 오프셋이 없을 경우 파티션의 맨 처음부터 모든 데이터를 읽음
            - none : 유효하지 않은 오프셋일 경우 exception 발생
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        /*
            파티션 할당 전략
            - Range(default): 컨슈머가 그룹에 속할 때, 각 컨슈머에게 연속적인 파티션 범위를 할당
                org.apache.kafka.clients.consumer.RangeAssignor
                - ex) 컨슈머 1: 파티션 0, 1
                      컨슈머 2: 파티션 2, 3
                      컨슈머 3: 파티션 4, 5
            - RR: 파티션을 컨슈머에게 순차적으로 할당
                org.apache.kafka.clints.consumer.RoundRobinAssignor
                - ex) 컨슈머 1: 파티션 0, 3
                      컨슈머 2: 파티션 1, 4
                      컨슈머 3: 파티션 2, 5
            - Sticky: 컨슈머가 파티션을 할당받을 때 이전 할당을 고려하여, 가능한 한 파티션을 변경하지 않도록 합니다.
                org.apache.kafka.clients.consumer.StickyAssignor
            - Cooperative Sticky: Sticky 전략과 비슷하지만, 협력적인 방식으로 작동하여 기존의 파티션 할당을 최대한 유지하면서 새로운 컨슈머를 추가하거나 제거할 수 있음
                org.apache.kafka.clients.consumer.CooperativeStickyAssignor
         */
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");

        /*
            ENABLE_AUTO_COMMIT_CONFIG : 자동으로 오프셋 커밋할지 말지..
            - true (default)
            - false
            아래 예제 있음
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribing to the topic 'customerCountries'
        consumer.subscribe(Collections.singletonList(TOPIC));

        Duration timeout = Duration.ofMillis(100);

        // 자동 오프셋 커밋 (basic)
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}