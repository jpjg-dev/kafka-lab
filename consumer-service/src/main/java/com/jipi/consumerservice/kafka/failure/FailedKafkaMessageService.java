package com.jipi.consumerservice.kafka.failure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jipi.consumerservice.kafka.KafkaCustomHeaders;
import com.jipi.consumerservice.kafka.event.StudyMessageCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailedKafkaMessageService {
    private final FailedKafkaMessageRepository failedKafkaMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void saveIfAbsent(FailedKafkaMessage failedKafkaMessage) {
        try {
            failedKafkaMessageRepository.saveAndFlush(failedKafkaMessage);
        } catch (DataIntegrityViolationException e) {
            log.warn(
                    "이미 저장된 DLT 실패 메시지입니다. originalTopic={}, partition={}, offset={}",
                    failedKafkaMessage.getOriginalTopic(),
                    failedKafkaMessage.getOriginalPartition(),
                    failedKafkaMessage.getOriginalOffset()
            );
        }
    }

    public void startRetry(Long failedMessageId) {
        // 재처리는 같은 실패 메시지에 대해 여러 요청이 동시에 들어올 수 있기 때문에,
        // 먼저 DB에서 상태를 '재처리 중'으로 선점해야 합니다.
        // 이렇게 해야 중복 재발행을 막고, 이미 처리 중인 메시지에 대해 다른 요청이 다시 작업하지 못합니다.
        // 이 메서드는 하나의 트랜잭션 안에서 상태를 선점하고, 재발행 결과에 따라 최종 상태를 남기도록 설계합니다.
        // 따라서 아래의 상태 변경과 예외 전파는 단순한 로그가 아니라 데이터 정합성을 맞추기 위한 핵심 흐름입니다.
        // 1. 재처리 권한 선점
        if (failedKafkaMessageRepository.updateToRetryingIfRetryable(failedMessageId, Instant.now()) == 0) {
            throw new IllegalStateException("이미 재처리 중이거나 재처리할 수 없는 메시지입니다. id=" + failedMessageId);
        }

        // 상태를 선점한 뒤에는 최신 실패 메시지를 다시 조회합니다.
        // 조회를 분리하는 이유는, 이후 단계에서 payload / topic / key 같은 원본 재발행 정보가
        // 반드시 DB에 저장된 값 기준으로 필요하기 때문입니다.
        // 또한 선점에 성공한 레코드가 실제로 존재하는지 다시 확인하는 검증 역할도 합니다.
        // 2. 실패 메세지 조회
        FailedKafkaMessage failedKafkaMessage =
                failedKafkaMessageRepository.findById(failedMessageId)
                        .orElseThrow(() -> new IllegalArgumentException("재처리할 수 없는 메시지입니다. id=" + failedMessageId));
        try {
            // DLT에는 원본 이벤트 객체가 아니라 JSON 문자열이 저장되어 있으므로,
            // 실제 Kafka로 다시 보낼 때는 애플리케이션이 이해할 수 있는 이벤트 타입으로 복원해야 합니다.
            // 그래야 직렬화 설정을 통해 다시 Kafka 메시지 바디로 변환할 수 있습니다.
            // 3. 저장된 JSON을 이벤트 객체로 복원
            StudyMessageCreatedEvent event =
                    objectMapper.readValue(failedKafkaMessage.getPayload(), StudyMessageCreatedEvent.class);

            // send()는 즉시 완료되는 동기 호출이 아니라 비동기 전송 결과를 담은 Future를 반환합니다.
            // 여기서 join()을 호출하는 이유는 실제 브로커 전송 성공/실패를 이 시점에 확정적으로 확인하기 위해서입니다.
            // join() 없이 끝내면 전송 실패가 나중에 발생해도 이 메서드는 성공한 것처럼 종료될 수 있고,
            // 그러면 재처리 상태와 실제 Kafka 전송 결과가 어긋날 수 있습니다.
            // 즉, join()은 "브로커에 실제로 전달되었는가"를 현재 흐름 안에서 확인하기 위한 동기 경계 역할을 합니다.
            // 이렇게 해야 성공 시에는 성공 상태로, 실패 시에는 실패 상태로 바로 분기할 수 있습니다.
            // 4. 원본 토빅으로 재발행
            ProducerRecord<String, Object> retryRecord =
                    new ProducerRecord<>(
                            failedKafkaMessage.getOriginalTopic(),
                            failedKafkaMessage.getMessageKey(),
                            event
                    );

            retryRecord.headers().add(
                    KafkaCustomHeaders.FAILED_MESSAGE_ID,
                    failedMessageId.toString().getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(retryRecord).join();
            log.info("Successfully retried failed Kafka message with ID: {}", failedKafkaMessage.getId());

            // 아래 상태 변경 주석은, 재발행이 성공했을 때 DB 상태를 어떻게 마무리할지 남겨둔 자리입니다.
            // 예를 들어 성공 처리 완료, 재처리 완료, 또는 이력 보관 같은 후속 상태 전환이 들어올 수 있습니다.
            // 5. 재발행 성공 상태 변경
            // 재발행 성공 후에는 DB에도 성공 완료 상태를 남겨서, 같은 메시지를 다시 재처리하지 않도록 막습니다.
            // 이 상태 변경이 있어야 운영자가 나중에 조회했을 때 "이미 재처리 완료된 메시지"인지 바로 알 수 있습니다.
            failedKafkaMessageRepository.markRepublished(failedMessageId);
            log.info("Kafka 메시지 재처리 성공. failedMessageId={}", failedMessageId);

        } catch (Exception e) {
            // 실패 시에는 재처리 중 상태를 실패 상태로 되돌리거나, 재시도 가능 횟수/에러 사유를 기록하는 로직이 들어갈 수 있습니다.
            //即, 여기의 주석은 단순 로그가 아니라 이후 상태 전환 정책을 연결할 지점이라는 의미입니다.
            // 6. 재발행 실패 상태 변경
            // 실패 상태를 남기는 이유는 단순히 에러를 기록하기 위해서가 아니라,
            // 재시도 정책, 원인 분석, 운영 대시보드 표시, 알람 트리거 등에 활용할 수 있게 하기 위함입니다.
            // 예외를 다시 던지는 이유는 호출자에게 실패를 명확히 전달하고,
            // 트랜잭션 범위 내에서는 실패한 흐름이 정상 완료로 오해되지 않게 하기 위해서입니다.
            failedKafkaMessageRepository.markRetryFailed(failedMessageId, e.getMessage());
            log.error("Kafka 메시지 재처리 실패. failedMessageId={}", failedKafkaMessage.getId(), e);
            throw new RuntimeException("KafKa 메세지 재처리에 실패했습니다.", e);
        }
    }

    public void markRetryFailed(
            Long failedMessageId,
            String exceptionMessage
    ) {
        int updatedCount =
                failedKafkaMessageRepository.markRetryFailed(
                        failedMessageId,
                        exceptionMessage
                );

        if (updatedCount == 0) {
            throw new IllegalStateException(
                    "재처리 실패 상태를 반영할 수 없습니다. id="
                            + failedMessageId
            );
        }
    }
}
