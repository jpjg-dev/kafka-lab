package com.jipi.consumerservice.kafka.event;

import java.time.Instant;

/*
 * Consumer가 Kafka JSON을 역직렬화해서 받을 객체
 *
 * Producer의 이벤트 클래스를 의존하지 않는다.
 * 다만 JSON 필드 이름과 타입은 호환되어야 한다.
 */
public record StudyMessageCreatedEvent(
        String eventId,
        String userId,
        String message,
        Instant occurredAt
) {
}
