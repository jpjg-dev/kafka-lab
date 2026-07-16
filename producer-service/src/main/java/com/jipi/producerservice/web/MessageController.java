package com.jipi.producerservice.web;

import com.jipi.producerservice.kafka.MessageProducer;
import com.jipi.producerservice.kafka.event.StudyMessageCreatedEvent;
import com.jipi.producerservice.message.StudyMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final StudyMessageService studyMessageService;

    @PostMapping
    public ResponseEntity<Void> create(@RequestBody MessageRequest messageRequest) {
        studyMessageService.createMessage(messageRequest.key(), messageRequest.message());

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }
}