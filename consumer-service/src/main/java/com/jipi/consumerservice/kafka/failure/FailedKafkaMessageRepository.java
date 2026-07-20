package com.jipi.consumerservice.kafka.failure;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public interface FailedKafkaMessageRepository extends JpaRepository<FailedKafkaMessage, Long> {

    // 15강: 재처리 가능한 상태만 RETRYING으로 변경하는 조건부 원자적 UPDATE
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            update FailedKafkaMessage message
               set message.status =
                       com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.RETRYING,
                   message.retryCount = message.retryCount + 1,
                   message.lastRetriedAt = :retriedAt,
                   message.lastRetryError = null
             where message.id = :id
               and (
                   message.status =
                       com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.PENDING
                   or message.status =
                       com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.RETRY_FAILED
               )
            """)
    int updateToRetryingIfRetryable(
            @Param("id") Long id,
            @Param("retriedAt") Instant retriedAt
    );

    // 15강: RETRYING 상태의 메시지만 REPUBLISHED로 완료 처리
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            update FailedKafkaMessage message
               set message.status =
                   com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.REPUBLISHED
             where message.id = :id
               and message.status =
                   com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.RETRYING
            """)
    int markRepublished(@Param("id") Long id);

    // 15강: 재발행 또는 재처리 메시지의 재실패 원인을 RETRY_FAILED 상태로 기록
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            update FailedKafkaMessage message
               set message.status =
                   com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.RETRY_FAILED,
                   message.lastRetryError = :errorMessage
             where message.id = :id
               and (
                   message.status =
                       com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.RETRYING
                   or
                   message.status =
                       com.jipi.consumerservice.kafka.failure.FailedKafkaMessageStatus.REPUBLISHED
               )
            """)
    int markRetryFailed(
            @Param("id") Long id,
            @Param("errorMessage") String errorMessage
    );
}
