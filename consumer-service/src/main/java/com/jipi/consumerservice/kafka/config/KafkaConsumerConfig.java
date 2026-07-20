package com.jipi.consumerservice.kafka.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        /*
         * 11~12강: 재시도를 모두 소진한 메시지를 기본 DLT 토픽으로 발행한다.
         * 원본 토픽이 study.message.created.v1이면 DLT는 study.message.created.v1-dlt다.
         */
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        /*
         * 10강: 2초 간격으로 최대 2회 재시도한다.
         * 최초 처리 1회와 재시도 2회를 합쳐 메시지 처리 시도는 최대 3회다.
         */
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 2L);

        // 10~11강: 처리 실패 → 재시도 → 재시도 소진 → DLT 발행
        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }
}
