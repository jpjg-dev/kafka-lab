package com.jipi.producerservice.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jipi.producerservice.kafka.event.StudyMessageCreatedEvent;
import com.jipi.producerservice.outbox.OutboxEvent;
import com.jipi.producerservice.outbox.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class StudyMessageService {
    private final StudyMessageRepository studyMessageRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    @Value("${app.kafka.topic.study-events}")
    private String studyEventsTopic;

    // 16강: 비즈니스 데이터와 Outbox 이벤트를 하나의 로컬 트랜잭션으로 저장
    @Transactional
    public void createMessage(String userId, String message) {
        // 16강: 먼저 실제 비즈니스 데이터를 저장
        StudyMessage studyMessage = studyMessageRepository.save(StudyMessage.create(userId, message));

        // 16강: Kafka로 발행할 도메인 이벤트 생성
        StudyMessageCreatedEvent event =
                new StudyMessageCreatedEvent(UUID.randomUUID().toString(), userId, message, Instant.now());
        String payload;
        try {
            // 16강: 이벤트를 Outbox 테이블에 보관할 JSON 문자열로 직렬화
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Outbox 이벤트 직렬화에 실패했습니다.", e);
        }

        // 16강: 비즈니스 데이터와 같은 트랜잭션 안에서 PENDING Outbox 이벤트 저장
        outboxEventRepository.save(OutboxEvent.create(
                event.eventId(), studyEventsTopic, studyMessage.getUserId(), payload));
    }
}
