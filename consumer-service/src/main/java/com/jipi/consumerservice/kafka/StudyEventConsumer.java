package com.jipi.consumerservice.kafka;

import com.jipi.consumerservice.kafka.event.StudyMessageCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StudyEventConsumer {
    /*
     * @KafkaListener
     *
     * 이 메서드가 Kafka 메시지를 소비하도록 등록한다.
     *
     * topics:
     * application.yml의 app.kafka.topic.study-events 값을 사용한다.
     * 현재 값은 study.events.v1이다.
     *
     * groupId:
     * application.yml의 spring.kafka.consumer.group-id 값을 사용한다.
     * 현재 값은 study-consumer-group이다.
     */
    @KafkaListener(
            topics = "${app.kafka.topic.study-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, StudyMessageCreatedEvent> consumerRecord) {
        /*
         * Kafka Record의 Value가 더 이상 String이 아니다.
         *
         * JacksonJsonDeserializer가 JSON을
         * StudyMessageCreatedEvent 객체로 변환했다.
         */
        StudyMessageCreatedEvent event = consumerRecord.value();

        log.info(
                """
                
                JSON Kafka 이벤트 수신
                topic={}
                partition={}
                offset={}
                key={}
                eventId={}
                userId={}
                message={}
                occurredAt={}
                """,
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                event.eventId(),
                event.userId(),
                event.message(),
                event.occurredAt()
        );
    }
}
