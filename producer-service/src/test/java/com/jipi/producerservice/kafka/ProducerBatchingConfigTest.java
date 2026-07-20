package com.jipi.producerservice.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 21강: batch.size, linger.ms, compression.type 설정 조합 검증
class ProducerBatchingConfigTest {

    @Test
    void 배치_linger_압축_설정이_올바르면_정상적으로_생성된다() {
        Map<String, Object> properties =
                validBatchingProducerProperties();

        ProducerConfig producerConfig =
                new ProducerConfig(properties);

        assertThat(
                producerConfig.getInt(
                        ProducerConfig.BATCH_SIZE_CONFIG
                )
        ).isEqualTo(32768);

        assertThat(
                producerConfig.getLong(
                        ProducerConfig.LINGER_MS_CONFIG
                )
        ).isEqualTo(10L);

        assertThat(
                producerConfig.getString(
                        ProducerConfig.COMPRESSION_TYPE_CONFIG
                )
        ).isEqualTo("lz4");
    }

    @Test
    void batch_size가_0이면_배치_기능이_비활성화된다() {
        Map<String, Object> properties =
                validBatchingProducerProperties();

        properties.put(
                ProducerConfig.BATCH_SIZE_CONFIG,
                0
        );

        ProducerConfig producerConfig =
                new ProducerConfig(properties);

        assertThat(
                producerConfig.getInt(
                        ProducerConfig.BATCH_SIZE_CONFIG
                )
        ).isZero();
    }

    @Test
    void 지원하지_않는_압축_방식이면_설정_오류가_발생한다() {
        Map<String, Object> properties =
                validBatchingProducerProperties();

        properties.put(
                ProducerConfig.COMPRESSION_TYPE_CONFIG,
                "brotli"
        );

        assertThatThrownBy(
                () -> new ProducerConfig(properties)
        )
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ProducerConfig.COMPRESSION_TYPE_CONFIG
                );
    }

    // 21강: 배치와 압축을 활성화할 기본 Producer 설정
    private Map<String, Object> validBatchingProducerProperties() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
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

        properties.put(
                ProducerConfig.LINGER_MS_CONFIG,
                10
        );

        properties.put(
                ProducerConfig.COMPRESSION_TYPE_CONFIG,
                "lz4"
        );

        return properties;
    }
}
