package com.jipi.consumerservice.kafka.web;

import com.jipi.consumerservice.kafka.failure.FailedKafkaMessageRepository;
import com.jipi.consumerservice.kafka.failure.FailedKafkaMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// 15강: 저장된 실패 메시지를 운영자가 수동 재처리할 API
@RestController
@RequestMapping("/api/v1/failed")
@RequiredArgsConstructor
public class FailedKafkaMessageController {
    private final FailedKafkaMessageService failedKafkaMessageRetryService;

    // 15강: 실패 이력 ID를 기준으로 재처리 상태 선점과 Kafka 재발행 시작
    @PostMapping("/{id}/retry")
    public ResponseEntity<Void> retry(@PathVariable("id") Long id) {
        failedKafkaMessageRetryService.startRetry(id);
        return ResponseEntity.ok().build();
    }
}
