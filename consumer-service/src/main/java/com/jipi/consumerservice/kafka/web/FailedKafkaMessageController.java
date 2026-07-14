package com.jipi.consumerservice.kafka.web;

import com.jipi.consumerservice.kafka.failure.FailedKafkaMessageRepository;
import com.jipi.consumerservice.kafka.failure.FailedKafkaMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/failed")
@RequiredArgsConstructor
public class FailedKafkaMessageController {
    private final FailedKafkaMessageService failedKafkaMessageRetryService;

    @PostMapping("/{id}/retry")
    public ResponseEntity<Void> retry(@PathVariable("id") Long id) {
        failedKafkaMessageRetryService.startRetry(id);
        return ResponseEntity.ok().build();
    }
}
