package com.jipi.consumerservice.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {
    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        /*
         * 2초마다 재시도
         * 최대 100회 재시도
         *
         * 재시도하는 동안 Consumer를 직접 종료해서
         * Offset이 어디에 머무는지 확인하기 위한 실습 설정이다.
         */
        return new DefaultErrorHandler(new FixedBackOff(2000L, 100L));
    }
}
