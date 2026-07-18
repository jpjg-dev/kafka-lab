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
@EmbeddedKafka(
        partitions = 3,
        topics = MessageOrderingIntegrationTest.TOPIC
)
class MessageOrderingIntegrationTest {

    static final String TOPIC = "message-ordering-test";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void 같은_key의_메시지는_같은_partition에서_발행_순서대로_저장된다() throws Exception {
        Map<String, Object> producerProperties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true,
                ProducerConfig.ACKS_CONFIG,
                "all",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                5
        );

        List<RecordMetadata> metadataList = new ArrayList<>();

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(producerProperties)) {

            for (int sequence = 1; sequence <= 5; sequence++) {
                RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(
                                TOPIC,
                                "user-1",
                                "message-" + sequence
                        )
                ).get(10, TimeUnit.SECONDS);

                metadataList.add(metadata);
            }
        }

        int assignedPartition = metadataList.getFirst().partition();

        assertThat(metadataList)
                .extracting(RecordMetadata::partition)
                .containsOnly(assignedPartition);

        for (int index = 1; index < metadataList.size(); index++) {
            long previousOffset = metadataList.get(index - 1).offset();
            long currentOffset = metadataList.get(index).offset();

            assertThat(currentOffset).isEqualTo(previousOffset + 1);
        }

        List<ConsumerRecord<String, String>> records =
                consumeFromBeginning(assignedPartition, 5);

        assertThat(records)
                .extracting(ConsumerRecord::key)
                .containsOnly("user-1");

        assertThat(records)
                .extracting(ConsumerRecord::value)
                .containsExactly(
                        "message-1",
                        "message-2",
                        "message-3",
                        "message-4",
                        "message-5"
                );

        records.forEach(record ->
                System.out.println(
                        "partition="
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

    private List<ConsumerRecord<String, String>> consumeFromBeginning(
            int partition,
            int expectedCount
    ) {
        Map<String, Object> consumerProperties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "message-ordering-test-group",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        TopicPartition topicPartition =
                new TopicPartition(TOPIC, partition);

        List<ConsumerRecord<String, String>> records =
                new ArrayList<>();

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProperties)) {

            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));

            long deadlineNanos =
                    System.nanoTime()
                            + Duration.ofSeconds(10).toNanos();

            while (
                    records.size() < expectedCount
                            && System.nanoTime() < deadlineNanos
            ) {
                consumer.poll(Duration.ofMillis(500))
                        .forEach(records::add);
            }
        }

        return records;
    }
}