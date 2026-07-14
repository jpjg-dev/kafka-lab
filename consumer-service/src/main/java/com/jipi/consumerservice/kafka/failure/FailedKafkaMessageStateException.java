package com.jipi.consumerservice.kafka.failure;

public class FailedKafkaMessageStateException extends RuntimeException {

    public FailedKafkaMessageStateException(
            Long messageId,
            FailedKafkaMessageStatus currentStatus,
            String reason
    ) {
        super(
                "실패 메시지 상태 전이 오류. messageId=%s, currentStatus=%s, reason=%s"
                        .formatted(messageId, currentStatus, reason)
        );
    }
}

