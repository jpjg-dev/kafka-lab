package com.jipi.producerservice.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value("${app.kafka.topic.study-events}")
    private String topic;

    public CompletableFuture<SendResult<String, String>> sendMessage(String key, String message) {
        log.info("key={}, message={}", key, message);
        return kafkaTemplate.send(topic, key, message);
    }
}
