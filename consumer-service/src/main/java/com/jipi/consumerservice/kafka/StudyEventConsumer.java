package com.jipi.consumerservice.kafka;

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
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        /*
         * ConsumerRecord<String, String>
         *
         * 첫 번째 String:
         * Kafka 메시지 Key 타입
         *
         * 두 번째 String:
         * Kafka 메시지 Value 타입
         *
         * Producer가 StringSerializer를 사용했기 때문에
         * Consumer도 StringDeserializer를 사용하고
         * 결과적으로 String 타입으로 받는다.
         */
        log.info(
                """
                        
                        Kafka 메시지 수신
                        topic={}
                        partition={}
                        offset={}
                        key={}
                        value={}
                        """,

                // 메시지가 들어온 토픽 이름
                consumerRecord.topic(),

                // 메시지가 저장된 파티션 번호
                consumerRecord.partition(),

                // 해당 파티션 안에서의 메시지 위치
                consumerRecord.offset(),

                // Producer가 보낸 Key
                consumerRecord.key(),

                // Producer가 보낸 실제 메시지
                consumerRecord.value()
        );
    }
}
