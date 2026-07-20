package com.jipi.producerservice.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@EmbeddedKafka(
        partitions = 1,
        topics = ProducerBatchingMetricsIntegrationTest.TOPIC
)
class ProducerBatchingMetricsIntegrationTest {

    static final String TOPIC =
            "producer-batching-metrics-test";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void 여러_메시지를_비동기로_전송하면_배치와_압축이_적용된다()
            throws Exception {

        Map<String, Object> producerProperties =
                batchingProducerProperties();

        List<Future<RecordMetadata>> futures =
                new ArrayList<>();

        String payload =
                "batch-compression-payload-".repeat(40);

        try (
                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(
                                producerProperties
                        )
        ) {
            for (int sequence = 1;
                 sequence <= 500;
                 sequence++) {

                Future<RecordMetadata> future =
                        producer.send(
                                new ProducerRecord<>(
                                        TOPIC,
                                        "user-1",
                                        payload + sequence
                                )
                        );

                futures.add(future);
            }

            /*
             * send() 직후 get()이나 join()을 호출하지 않는다.
             *
             * 여러 레코드가 Producer의 RecordAccumulator에
             * 함께 들어가야 배치가 만들어질 수 있다.
             */
            producer.flush();

            for (Future<RecordMetadata> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

            double recordSendTotal =
                    metricValue(
                            producer,
                            "record-send-total"
                    );

            double recordsPerRequestAverage =
                    metricValue(
                            producer,
                            "records-per-request-avg"
                    );

            double batchSizeAverage =
                    metricValue(
                            producer,
                            "batch-size-avg"
                    );

            double compressionRateAverage =
                    metricValue(
                            producer,
                            "compression-rate-avg"
                    );

            double recordQueueTimeAverage =
                    metricValue(
                            producer,
                            "record-queue-time-avg"
                    );

            System.out.println(
                    "record-send-total="
                            + recordSendTotal
            );

            System.out.println(
                    "records-per-request-avg="
                            + recordsPerRequestAverage
            );

            System.out.println(
                    "batch-size-avg="
                            + batchSizeAverage
            );

            System.out.println(
                    "compression-rate-avg="
                            + compressionRateAverage
            );

            System.out.println(
                    "record-queue-time-avg="
                            + recordQueueTimeAverage
            );

            assertThat(recordSendTotal)
                    .isEqualTo(500.0);

            assertThat(recordsPerRequestAverage)
                    .isGreaterThan(1.0);

            assertThat(batchSizeAverage)
                    .isGreaterThan(0.0);

            assertThat(compressionRateAverage)
                    .isGreaterThan(0.0)
                    .isLessThan(1.0);

            assertThat(recordQueueTimeAverage)
                    .isGreaterThanOrEqualTo(0.0);
        }
    }

    private Map<String, Object> batchingProducerProperties() {
        Map<String, Object> properties =
                new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString()
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        properties.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                "batching-metrics-test-producer"
        );

        properties.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        properties.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        properties.put(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                5
        );

        properties.put(
                ProducerConfig.BATCH_SIZE_CONFIG,
                32768
        );

        /*
         * 로컬 통합 테스트에서 배치 동작을 확실하게
         * 관찰하기 위해 application.yaml보다 크게 설정한다.
         */
        properties.put(
                ProducerConfig.LINGER_MS_CONFIG,
                100
        );

        properties.put(
                ProducerConfig.COMPRESSION_TYPE_CONFIG,
                "lz4"
        );

        return properties;
    }

    private double metricValue(
            KafkaProducer<String, String> producer,
            String metricName
    ) {
        return producer.metrics()
                .entrySet()
                .stream()
                .filter(entry ->
                        entry.getKey()
                                .group()
                                .equals("producer-metrics")
                )
                .filter(entry ->
                        entry.getKey()
                                .name()
                                .equals(metricName)
                )
                .map(entry ->
                        ((Number) entry.getValue()
                                .metricValue())
                                .doubleValue()
                )
                .findFirst()
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Producer metric을 찾을 수 없습니다. metric="
                                        + metricName
                        )
                );
    }
}