package com.jipi.producerservice.web;

public record PublishResponse(
        String topic,
        int partition,
        long offset,
        String key,
        String eventId,
        String message
) {
}
