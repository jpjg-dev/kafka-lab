package com.jipi.consumerservice.kafka.failure;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public interface FailedKafkaMessageRepository extends JpaRepository<FailedKafkaMessage, Long> {

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
