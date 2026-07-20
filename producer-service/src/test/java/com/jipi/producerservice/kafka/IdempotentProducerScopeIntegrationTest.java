package com.jipi.producerservice.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// 19강: Idempotent Producer가 애플리케이션의 중복 send 호출까지 제거하지는 않는다는 범위 검증
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = IdempotentProducerScopeIntegrationTest.TOPIC)
class IdempotentProducerScopeIntegrationTest {

    static final String TOPIC = "idempotent-producer-scope-test";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void 같은_메시지를_send로_두번_호출하면_멱등_프로듀서여도_두건이_저장된다() throws Exception {
        Map<String, Object> producerProperties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5
        );

        // 19강: 동일한 Key와 Value라도 애플리케이션이 send를 두 번 호출하면 서로 다른 레코드로 발행
        RecordMetadata firstMetadata;
        RecordMetadata secondMetadata;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            firstMetadata = producer.send(
                    new ProducerRecord<>(TOPIC, "same-key", "same-message")
            ).get(10, TimeUnit.SECONDS);

            secondMetadata = producer.send(
                    new ProducerRecord<>(TOPIC, "same-key", "same-message")
            ).get(10, TimeUnit.SECONDS);
        }

        assertThat(firstMetadata.partition()).isEqualTo(secondMetadata.partition());
        assertThat(secondMetadata.offset()).isEqualTo(firstMetadata.offset() + 1);

        List<ConsumerRecord<String, String>> records = consumeFromBeginning(2);

        assertThat(records).hasSize(2);
        assertThat(records)
                .extracting(ConsumerRecord::key)
                .containsExactly("same-key", "same-key");
        assertThat(records)
                .extracting(ConsumerRecord::value)
                .containsExactly("same-message", "same-message");
        System.out.println(
                "첫 번째 메시지: partition="
                        + firstMetadata.partition()
                        + ", offset="
                        + firstMetadata.offset()
        );

        System.out.println(
                "두 번째 메시지: partition="
                        + secondMetadata.partition()
                        + ", offset="
                        + secondMetadata.offset()
        );
        records.forEach(record ->
                System.out.println(
                        "수신 메시지: partition="
                                + record.partition()
                                + ", offset="
                                + record.offset()
                                + ", key="
                                + record.key()
                                + ", value="
                                + record.value()
                )
        );
    }


    // 19강: Embedded Kafka의 첫 Offset부터 두 레코드를 직접 조회
    private List<ConsumerRecord<String, String>> consumeFromBeginning(int expectedCount) {
        Map<String, Object> consumerProperties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "idempotent-producer-scope-test-group",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));

            long deadlineNanos = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (records.size() < expectedCount && System.nanoTime() < deadlineNanos) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }

        return records;
    }
}
