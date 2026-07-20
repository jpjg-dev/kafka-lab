package com.jipi.producerservice.kafka;

import com.jipi.producerservice.kafka.event.StudyMessageCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

// 2강: Spring KafkaTemplate을 이용한 기본 Producer 구현
@Service
@Slf4j
@RequiredArgsConstructor
public class MessageProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    @Value("${app.kafka.topic.study-events}")
    private String topic;

    // 2~4강: Key와 이벤트를 지정한 토픽으로 비동기 발행
    public CompletableFuture<SendResult<String, Object>> sendMessage(String key, StudyMessageCreatedEvent event) {
        log.info("key={}, event={}", key, event);
        return kafkaTemplate.send(topic, key, event);
    }
}
