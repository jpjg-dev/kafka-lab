package com.jipi.producerservice.web;

import com.jipi.producerservice.kafka.MessageProducer;
import com.jipi.producerservice.kafka.event.StudyMessageCreatedEvent;
import lombok.RequiredArgsConstructor;
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

    private final MessageProducer messageProducer;

    @PostMapping
    public CompletableFuture<PublishResponse> publish(@RequestBody MessageRequest messageRequest) {
        /*
         * HTTP 요청 DTO를 Kafka 이벤트 객체로 변환한다.
         *
         * eventId:
         * 이벤트 한 건마다 새로운 UUID 생성
         *
         * userId:
         * 요청에서 받은 key 사용
         *
         * occurredAt:
         * 이벤트를 생성한 현재 시각
         */
        StudyMessageCreatedEvent event = new StudyMessageCreatedEvent(
                UUID.randomUUID().toString(),
                messageRequest.key(),
                messageRequest.message(),
                Instant.now()
        );
        /*
         * Kafka Key는 userId를 사용한다.
         *
         * 같은 userId는 같은 파티션으로 들어가므로
         * 사용자별 이벤트 순서를 유지할 수 있다.
         */
        return messageProducer.sendMessage(messageRequest.key(), event)
                .thenApply(r -> {
                    var metadata = r.getRecordMetadata();
                    return new PublishResponse(
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            messageRequest.key(),
                            event.eventId(),
                            event.message()
                    );
                });
    }
}
