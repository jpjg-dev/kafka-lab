package com.jipi.producerservice.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxEventPublisher {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 17강: 설정된 주기마다 오래된 PENDING 이벤트를 최대 100건씩 조회
    @Scheduled(fixedDelayString = "${app.outbox.publisher.fixed-delay-ms}")
    @Transactional
    public void publishPendingEvents() {
        outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING).forEach(this::publish);
    }

    private void publish(OutboxEvent outboxEvent) {
        try {
            // 17강: Outbox에 저장한 JSON 문자열을 Kafka 발행용 JSON 객체로 복원
            JsonNode payload =
                    objectMapper.readTree(outboxEvent.getPayload());

            // 17~18강: Broker ACK 결과를 확인하기 위해 비동기 전송 결과를 join으로 대기
            SendResult<String, Object> sendResult = kafkaTemplate.send(
                    outboxEvent.getTopic(),
                    outboxEvent.getMessageKey(),
                    payload
            ).join();
            
            RecordMetadata recordMetadata = sendResult.getRecordMetadata();

            // 17~18강: Kafka 발행 성공을 확인한 뒤 Outbox 상태를 PUBLISHED로 변경
            outboxEvent.markPublished();

            log.info(
                    "Outbox 이벤트 발행 성공. eventId={}, topic={}, key={}, partition={}, offset={}",
                    outboxEvent.getEventId(),
                    recordMetadata.topic(),
                    outboxEvent.getMessageKey(),
                    recordMetadata.partition(),
                    recordMetadata.offset()
            );
        } catch (Exception e) {
            // 17강: 실패한 이벤트는 PENDING 상태로 남겨 다음 스케줄에서 다시 시도
            log.error(
                    "Outbox 이벤트 발행 실패. eventId={}, topic={}",
                    outboxEvent.getEventId(),
                    outboxEvent.getTopic(),
                    e
            );
        }
    }
}
