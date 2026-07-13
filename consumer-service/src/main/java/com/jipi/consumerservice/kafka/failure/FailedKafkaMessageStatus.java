package com.jipi.consumerservice.kafka.failure;

public enum FailedKafkaMessageStatus {
    // 아직 재처리하지 않은 실패 메세지
    PENDING,
    // 현재 재처리 중인 실패 메세지
    RETRYING,
    // 재처리 성공
    RESOLVED,
    // 재처리했지만 다시 실패한 메세지
    RETRY_FAILED
}
