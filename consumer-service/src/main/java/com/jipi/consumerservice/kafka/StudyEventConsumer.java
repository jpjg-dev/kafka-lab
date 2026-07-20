package com.jipi.consumerservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jipi.consumerservice.kafka.event.StudyMessageCreatedEvent;
import com.jipi.consumerservice.kafka.failure.FailedKafkaMessage;
import com.jipi.consumerservice.kafka.failure.FailedKafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
@RequiredArgsConstructor
public class StudyEventConsumer {
    private final FailedKafkaMessageService failedKafkaMessageService;
    private final ObjectMapper objectMapper;

    // 3~9강: 원본 토픽의 JSON 이벤트를 Consumer Group 단위로 소비
    @KafkaListener(
            topics = "${app.kafka.topic.study-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, StudyMessageCreatedEvent> consumerRecord) {

        StudyMessageCreatedEvent event = consumerRecord.value();
        log.info(
                "메시지 처리 시작: userId={}, partition={}, offset={}, eventId={}, message={}",
                consumerRecord.key(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                event.eventId(),
                event.message()
        );

        // 9~10강: 재시도와 DLT 흐름을 확인하기 위한 의도적 Consumer 오류
        if (event.message().contains("fail")) {
            throw new RuntimeException("의도적으로 발생시킨 Consumer 처리 오류");
        }
        log.info(
                "메세지 처리완료: userId={}, partition={}, offset={}",
                consumerRecord.key(),
                consumerRecord.partition(),
                consumerRecord.offset()
        );
    }

    // 11~14강: 재시도를 소진해 DLT로 이동한 메시지를 소비하고 실패 이력 저장
    @KafkaListener(
            topics = "${app.kafka.topic.study-events-dlt}",
            groupId = "study-json-dlt-consumer-group"
    )
    public void consumeDlt(
            ConsumerRecord<String, StudyMessageCreatedEvent> consumerRecord,
            @Header(KafkaHeaders.DLT_ORIGINAL_TOPIC) String originalTopic,
            @Header(KafkaHeaders.DLT_ORIGINAL_PARTITION) int originalPartition,
            @Header(KafkaHeaders.DLT_ORIGINAL_OFFSET) long originalOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionType,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaCustomHeaders.FAILED_MESSAGE_ID, required = false) byte[] failedMessageIdHeader
    ) {
        StudyMessageCreatedEvent event = consumerRecord.value();
        String payload;

        // 15강: 재처리 메시지가 다시 실패하면 신규 이력을 만들지 않고 기존 상태를 갱신
        if (failedMessageIdHeader != null) {
            Long failedMessageId = Long.valueOf(
                    new String(
                            failedMessageIdHeader,
                            StandardCharsets.UTF_8
                    )
            );

            failedKafkaMessageService.markRetryFailed(
                    failedMessageId,
                    exceptionMessage
            );

            log.warn(
                    "재처리 메시지가 다시 실패했습니다. failedMessageId={}",
                    failedMessageId
            );

            return;
        }

        // 13강: DLT 메시지 값을 DB에 저장할 JSON 문자열로 변환
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("Failed to serialize event to JSON", e);
            payload = event.toString();
        }

        // 14강 실습용: 복합 Unique Constraint 위반을 재현할 때만 주석 해제
//        originalTopic = "study.message.created.v1";
//        originalPartition = 0;
//        originalOffset = 0L;

        // 13~14강: 원본 topic·partition·offset을 기준으로 실패 이력을 멱등 저장
        failedKafkaMessageService.saveIfAbsent(FailedKafkaMessage.create(
                originalTopic,
                originalPartition,
                originalOffset,
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                payload,
                event.eventId(),
                exceptionType,
                exceptionMessage
        ));
        log.info(
                """
                        DLT 메시지 수신
                        topic={}
                        partition={}
                        offset={}
                        key={}
                        eventId={}
                        message={}
                        """,
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                event.eventId(),
                event.message()
        );
    }
}
