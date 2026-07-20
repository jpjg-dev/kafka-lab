package com.jipi.producerservice.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 19강: Idempotent Producer 필수 설정 조합과 잘못된 조합을 검증
class IdempotentProducerConfigTest {

    @Test
    void 멱등성_설정이_올바르면_정상적으로_생성된다() {
        Map<String, Object> properties = validIdempotentProducerProperties();

        ProducerConfig producerConfig = new ProducerConfig(properties);

        assertThat(producerConfig.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isTrue();
        assertThat(producerConfig.getString(ProducerConfig.ACKS_CONFIG)).isEqualTo("-1");
        assertThat(producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)).isEqualTo(5);
    }

    @Test
    void 멱등성이_활성화된_상태에서_acks가_1이면_설정_오류가_발생한다() {
        Map<String, Object> properties = validIdempotentProducerProperties();
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        assertThatThrownBy(() -> new ProducerConfig(properties))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("acks");
    }

    @Test
    void 멱등성이_활성화된_상태에서_max_in_flight가_5를_초과하면_설정_오류가_발생한다() {
        Map<String, Object> properties = validIdempotentProducerProperties();
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 6);

        assertThatThrownBy(() -> new ProducerConfig(properties))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("max.in.flight.requests.per.connection");
    }

    // 19강: enable.idempotence=true에 필요한 acks와 max.in.flight 기본 조합
    private Map<String, Object> validIdempotentProducerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return properties;
    }
}
