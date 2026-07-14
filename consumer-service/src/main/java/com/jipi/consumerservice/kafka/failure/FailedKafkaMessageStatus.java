package com.jipi.consumerservice.kafka.failure;

public enum FailedKafkaMessageStatus {
    // 아직 재처리하지 않은 실패 메세지
    PENDING,

    // 현재 재처리 중인 실패 메세지
    RETRYING,

    // Kafka 원본 토픽으로 다시 발행된 메시지
    REPUBLISHED,

    // Kafka 재발행 자체가 실패한 메시지
    RETRY_FAILED
}
