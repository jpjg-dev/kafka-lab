package com.jipi.producerservice.outbox;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

// 16강: Kafka 발행 전 이벤트를 로컬 트랜잭션으로 저장할 Outbox 엔티티
@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(
        name = "outbox_event",
        uniqueConstraints = {
                // 16강: 같은 eventId의 Outbox 이벤트가 중복 저장되지 않도록 보장
                @UniqueConstraint(
                        name = "uk_outbox_event_event_id",
                        columnNames = "event_id"
                )
        }
)
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(nullable = false, length = 200)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 100)
    private String messageKey;

    @Lob
    @Column(nullable = false)
    private String payload;

    // 16~17강: Kafka 발행 전 PENDING, 발행 성공 후 PUBLISHED 상태 관리
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private OutboxEventStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    // 17~18강: Broker 발행 성공을 확인한 경우에만 발행 완료 상태로 변경
    public void markPublished() {
        if (this.status != OutboxEventStatus.PENDING) {
            throw new IllegalStateException(
                    "PENDING 상태의 이벤트만 발행 완료 처리할 수 있습니다. eventId="
                            + eventId
            );
        }

        this.status = OutboxEventStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    private OutboxEvent(
            String eventId,
            String topic,
            String messageKey,
            String payload
    ) {
        this.eventId = eventId;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.status = OutboxEventStatus.PENDING;
        this.createdAt = Instant.now();
    }

    public static OutboxEvent create(
            String eventId,
            String topic,
            String messageKey,
            String payload
    ) {
        return new OutboxEvent(
                eventId,
                topic,
                messageKey,
                payload
        );
    }

}
