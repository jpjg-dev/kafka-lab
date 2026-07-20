package com.jipi.consumerservice.kafka.failure;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

// 13강: DLT로 이동한 메시지와 원본 위치, 예외 정보를 저장하는 실패 이력 엔티티
@Entity
@Table(name = "failed_kafka_message",
        uniqueConstraints = {
                // 14강: 동일한 원본 Kafka 레코드의 실패 이력이 중복 저장되지 않도록 보장
                @UniqueConstraint(
                        name = "uk_failed_kafka_message_original_position",
                        columnNames = {
                                "original_topic", "original_partition", "original_offset"
                        })})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class FailedKafkaMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "original_topic", nullable = false, length = 200)
    private String originalTopic;

    @Column(name = "original_partition", nullable = false)
    private Integer originalPartition;

    @Column(name = "original_offset", nullable = false)
    private Long originalOffset;

    @Column(name = "dlt_topic", nullable = false, length = 200)
    private String dltTopic;

    @Column(name = "dlt_partition", nullable = false)
    private Integer dltPartition;

    @Column(name = "dlt_offset", nullable = false)
    private Long dltOffset;

    @Column(name = "message_key", length = 255)
    private String messageKey;

    @Lob
    @Column(nullable = false)
    private String payload;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "exception_type", length = 500)
    private String exceptionType;

    @Column(name = "exception_message", length = 2000)
    private String exceptionMessage;

    // 15강: PENDING, RETRYING, REPUBLISHED, RETRY_FAILED 재처리 상태 관리
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private FailedKafkaMessageStatus status;

    @Column(name = "failed_at", nullable = false, updatable = false)
    private Instant failedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_retried_at")
    private Instant lastRetriedAt;

    @Column(name = "last_retry_error", length = 2000)
    private String lastRetryError;

    private FailedKafkaMessage(
            String originalTopic,
            Integer originalPartition,
            Long originalOffset,
            String dltTopic,
            Integer dltPartition,
            Long dltOffset,
            String messageKey,
            String payload,
            String eventId,
            String exceptionType,
            String exceptionMessage
    ) {
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.dltTopic = dltTopic;
        this.dltPartition = dltPartition;
        this.dltOffset = dltOffset;
        this.messageKey = messageKey;
        this.payload = payload;
        this.eventId = eventId;
        this.exceptionType = exceptionType;
        this.exceptionMessage = exceptionMessage;
        this.status = FailedKafkaMessageStatus.PENDING;
        this.failedAt = Instant.now();
        this.retryCount = 0;

    }

    public static FailedKafkaMessage create(
            String originalTopic,
            Integer originalPartition,
            Long originalOffset,
            String dltTopic,
            Integer dltPartition,
            Long dltOffset,
            String messageKey,
            String payload,
            String eventId,
            String exceptionType,
            String exceptionMessage
    ) {
        return new FailedKafkaMessage(
                originalTopic,
                originalPartition,
                originalOffset,
                dltTopic,
                dltPartition,
                dltOffset,
                messageKey,
                payload,
                eventId,
                exceptionType,
                exceptionMessage
        );
    }
}
