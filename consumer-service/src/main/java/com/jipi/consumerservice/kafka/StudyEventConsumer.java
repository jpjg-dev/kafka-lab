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
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("Failed to serialize event to JSON", e);
            payload = event.toString();
        }
//         의도적인 복합유니크 제약 위반
//        originalTopic = "study.message.created.v1";
//        originalPartition = 0;
//        originalOffset = 0L;

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
