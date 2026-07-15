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

    @Transactional
    public void creatMessage(String userId, String message) {
        StudyMessage studyMessage = studyMessageRepository.save(StudyMessage.create(userId, message));

        StudyMessageCreatedEvent event =
                new StudyMessageCreatedEvent(UUID.randomUUID().toString(), userId, message, Instant.now());
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Outbox 이벤트 직렬화에 실패했습니다.", e);
        }

        outboxEventRepository.save(OutboxEvent.create(
                event.eventId(), studyEventsTopic, studyMessage.getId().toString(), payload));
    }
}
