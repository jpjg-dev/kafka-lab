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
         * 재시도를 모두 실패한 메시지를
         * DLT 토픽으로 발행하는 Recoverer다.
         *
         * 기본 토픽 이름 규칙:
         *
         * 원본:
         * study.message.created.v1
         *
         * 실패:
         * study.message.created.v1.DLT
         */
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        /*
         * 2초마다 재시도
         * 최대 100회 재시도
         *
         * 재시도하는 동안 Consumer를 직접 종료해서
         * Offset이 어디에 머무는지 확인하기 위한 실습 설정이다.
         */
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 2L);
        /*
         * 처리 실패
         * → FixedBackOff 정책에 따라 재시도
         * → 재시도 소진
         * → DeadLetterPublishingRecoverer 실행
         * → DLT 발행
         */
        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }
}
