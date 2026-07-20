package com.jipi.producerservice.kafka.event;

import java.time.Instant;

/*
 * 7강: Kafka로 발행할 JSON 이벤트 모델
 * Jackson JsonSerializer가 이 객체를 JSON byte[]로 변환한다.
 */
public record StudyMessageCreatedEvent(
        /* 7강: 이벤트 한 건을 식별할 고유 ID */
        String eventId,

        /* 4강: Kafka Key로도 사용할 사용자 ID */
        String userId,

        /* 7강: 실제 메시지 내용 */
        String message,

        /* 7강: 이벤트가 발생한 UTC 기준 절대 시각 */
        Instant occurredAt
) {
}
