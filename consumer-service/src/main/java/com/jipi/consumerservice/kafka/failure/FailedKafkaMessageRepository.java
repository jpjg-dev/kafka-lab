package com.jipi.consumerservice.kafka.failure;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedKafkaMessageRepository extends JpaRepository<FailedKafkaMessage, Long> {
//    boolean existsByOriginalTopicAndOriginalPartitionAndDltOffset(String originalTopic, int partition, long dltOffset);
}
