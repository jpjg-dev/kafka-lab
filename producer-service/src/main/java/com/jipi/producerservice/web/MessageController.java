package com.jipi.producerservice.web;

import com.jipi.producerservice.kafka.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping
    public CompletableFuture<PublishResponse> publish(@RequestBody MessageRequest messageRequest) {

        return messageProducer.sendMessage(messageRequest.key(), messageRequest.message())
                .thenApply(r -> {
                    var metadata = r.getRecordMetadata();
                    return new PublishResponse(
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            messageRequest.key(),
                            messageRequest.message()
                    );
                });
    }
}
