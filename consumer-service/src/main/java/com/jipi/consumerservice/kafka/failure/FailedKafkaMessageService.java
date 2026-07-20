package com.jipi.consumerservice.kafka.failure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jipi.consumerservice.kafka.KafkaCustomHeaders;
import com.jipi.consumerservice.kafka.event.StudyMessageCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailedKafkaMessageService {
    private final FailedKafkaMessageRepository failedKafkaMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 14강: 원본 topic·partition·offset 복합 Unique Constraint로 실패 이력 중복 저장 방지
    public void saveIfAbsent(FailedKafkaMessage failedKafkaMessage) {
        try {
            failedKafkaMessageRepository.saveAndFlush(failedKafkaMessage);
        } catch (DataIntegrityViolationException e) {
            log.warn(
                    "이미 저장된 DLT 실패 메시지입니다. originalTopic={}, partition={}, offset={}",
                    failedKafkaMessage.getOriginalTopic(),
                    failedKafkaMessage.getOriginalPartition(),
                    failedKafkaMessage.getOriginalOffset()
            );
        }
    }

    // 15강: 실패 메시지 상태 선점부터 Kafka 재발행 결과 반영까지 처리
    public void startRetry(Long failedMessageId) {
        // 15강: 조건부 UPDATE로 PENDING 또는 RETRY_FAILED 상태를 RETRYING으로 원자적 선점
        if (failedKafkaMessageRepository.updateToRetryingIfRetryable(failedMessageId, Instant.now()) == 0) {
            throw new IllegalStateException("이미 재처리 중이거나 재처리할 수 없는 메시지입니다. id=" + failedMessageId);
        }

        // 15강: 선점 성공 후 DB에 저장된 원본 topic·key·payload를 다시 조회
        FailedKafkaMessage failedKafkaMessage =
                failedKafkaMessageRepository.findById(failedMessageId)
                        .orElseThrow(() -> new IllegalArgumentException("재처리할 수 없는 메시지입니다. id=" + failedMessageId));
        try {
            // 15강: 저장된 JSON payload를 원본 이벤트 객체로 복원
            StudyMessageCreatedEvent event =
                    objectMapper.readValue(failedKafkaMessage.getPayload(), StudyMessageCreatedEvent.class);

            // 15강: 기존 실패 이력 ID를 커스텀 헤더에 넣어 원본 토픽으로 재발행
            ProducerRecord<String, Object> retryRecord =
                    new ProducerRecord<>(
                            failedKafkaMessage.getOriginalTopic(),
                            failedKafkaMessage.getMessageKey(),
                            event
                    );

            retryRecord.headers().add(
                    KafkaCustomHeaders.FAILED_MESSAGE_ID,
                    failedMessageId.toString().getBytes(StandardCharsets.UTF_8));

            // 15강: Broker 전송 성공 여부를 현재 재처리 흐름 안에서 확정
            kafkaTemplate.send(retryRecord).join();
            log.info("Successfully retried failed Kafka message with ID: {}", failedKafkaMessage.getId());

            // 15강: 재발행 성공 시 REPUBLISHED 상태로 변경해 중복 재처리 방지
            if (failedKafkaMessageRepository.markRepublished(failedMessageId) == 1) {
                log.info("Kafka 메시지 재처리 성공. failedMessageId={}", failedMessageId);
            }
        } catch (Exception e) {
            // 15강: 재발행 실패 원인을 기록하고 RETRY_FAILED 상태로 변경
            failedKafkaMessageRepository.markRetryFailed(failedMessageId, e.getMessage());
            log.error("Kafka 메시지 재처리 실패. failedMessageId={}", failedKafkaMessage.getId(), e);
            throw new RuntimeException("KafKa 메세지 재처리에 실패했습니다.", e);
        }
    }

    // 15강: 재처리 메시지가 DLT로 다시 이동했을 때 기존 실패 이력 상태 갱신
    public void markRetryFailed(
            Long failedMessageId,
            String exceptionMessage
    ) {
        int updatedCount =
                failedKafkaMessageRepository.markRetryFailed(
                        failedMessageId,
                        exceptionMessage
                );

        if (updatedCount == 0) {
            throw new IllegalStateException(
                    "재처리 실패 상태를 반영할 수 없습니다. id="
                            + failedMessageId
            );
        }
    }
}
