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

    @Scheduled(fixedDelayString = "${app.outbox.publisher.fixed-delay-ms}")
    @Transactional
    public void publishPendingEvents() {
        outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING).forEach(this::publish);
    }

    private void publish(OutboxEvent outboxEvent) {
        try {
            JsonNode payload =
                    objectMapper.readTree(outboxEvent.getPayload());

            SendResult<String, Object> sendResult = kafkaTemplate.send(
                    outboxEvent.getTopic(),
                    outboxEvent.getMessageKey(),
                    payload
            ).join();
            RecordMetadata recordMetadata = sendResult.getRecordMetadata();

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
            log.error(
                    "Outbox 이벤트 발행 실패. eventId={}, topic={}",
                    outboxEvent.getEventId(),
                    outboxEvent.getTopic(),
                    e
            );
        }
    }
}
