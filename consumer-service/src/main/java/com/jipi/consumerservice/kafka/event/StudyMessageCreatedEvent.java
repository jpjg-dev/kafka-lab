package com.jipi.consumerservice.kafka.event;

import java.time.Instant;

/*
 * 7강: Consumer가 Kafka JSON을 역직렬화해서 받을 이벤트 모델
 * Producer 클래스를 직접 의존하지 않고 JSON 필드 이름과 타입만 호환한다.
 */
public record StudyMessageCreatedEvent(
        String eventId,
        String userId,
        String message,
        Instant occurredAt
) {
}
