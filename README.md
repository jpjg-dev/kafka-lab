# Kafka Lab

Kafka의 핵심 개념과 메시지 처리 흐름을 직접 구현하며 학습한 내용을 기록하는 실습 저장소입니다.

Docker Compose 기반의 Kafka 환경에서 Spring Boot Producer와 Consumer를 구성하고, Topic·Partition·Offset·Consumer Group과 같은 기본 개념부터 재시도, DLT, 멱등성, Transactional Outbox Pattern까지 단계적으로 구현합니다. 학습이 진행됨에 따라 코드와 실습 내용이 계속 추가됩니다.

<details>
<summary><strong>강의 순서 보기</strong></summary>

1. Docker Compose 기반 Kafka 환경 구성
2. Spring Boot Producer 구현
3. Spring Boot Consumer 구현
4. Topic, Partition, Key, Offset 기초
5. Consumer Group과 파티션 분산 처리
6. Consumer Rebalancing 실습
7. JSON 이벤트 직렬화·역직렬화
8. Offset Commit과 AckMode.RECORD
9. Consumer 장애 발생과 재처리 확인
10. DefaultErrorHandler와 FixedBackOff
11. DeadLetterPublishingRecoverer와 DLT 구성
12. 기본 DLT 토픽 규칙(`원본토픽-dlt`) 확인
13. H2·JPA 연결 및 DLT 실패 이력 저장
14. 실패 이력 중복 저장 방지와 멱등성
15. 실패 메시지 상태 관리와 재처리 API
16. Transactional Outbox Pattern 기초
17. Outbox Publisher와 Kafka 발행
18. Producer ACK와 내구성 설정
19. Idempotent Producer
20. 메시지 순서 보장
21. Batch, Linger, Compression
22. Kafka Monitoring과 Consumer Lag
23. Kafka Cluster와 Replication
24. Kubernetes 기반 Kafka 운영

</details>
