package com.jipi.producerservice.kafka.event;

import java.time.Instant;

/*
 * Kafka로 발행할 이벤트 객체
 *
 * 이 Java 객체는 JacksonJsonSerializer를 통해
 * JSON byte[]로 변환되어 Kafka에 저장된다.
 */
public record StudyMessageCreatedEvent(
        /*
         * 이벤트 자체를 식별하는 고유 ID
         *
         * userId는 사용자를 식별하고,
         * eventId는 발생한 이벤트 한 건을 식별한다.
         */
        String eventId,

        /*
         * 이 이벤트와 관련된 사용자 ID
         *
         * Kafka Key로도 같은 값을 사용할 예정이다.
         */
        String userId,

        /*
         * 실제 메시지 내용
         */
        String message,

        /*
         * 이벤트가 발생한 시각
         *
         * Instant는 UTC 기준 절대 시각을 표현한다.
         */
        Instant occurredAt
) {
}
