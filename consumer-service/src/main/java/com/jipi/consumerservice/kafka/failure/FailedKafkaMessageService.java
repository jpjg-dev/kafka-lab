package com.jipi.consumerservice.kafka.failure;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailedKafkaMessageService {
    private final FailedKafkaMessageRepository failedKafkaMessageRepository;

    public void saveIfAbsent(FailedKafkaMessage failedKafkaMessage) {
        try {
            failedKafkaMessageRepository.saveAndFlush(failedKafkaMessage);
            log.info("save success.");
        } catch (DataIntegrityViolationException e) {
            log.warn("Failed to save failed Kafka message. Message already exists. {}", e.getMessage());
        }
    }
}
