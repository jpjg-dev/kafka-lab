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
    }

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
