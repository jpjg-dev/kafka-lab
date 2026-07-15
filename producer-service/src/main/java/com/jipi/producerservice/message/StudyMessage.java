package com.jipi.producerservice.message;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "study_message")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StudyMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, length = 100)
    private String userId;

    @Column(nullable = false, length = 1000)
    private String message;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    private StudyMessage(
            String userId,
            String message
    ) {
        this.userId = userId;
        this.message = message;
        this.createdAt = Instant.now();
    }

    public static StudyMessage create(
            String userId,
            String message
    ) {
        return new StudyMessage(userId, message);
    }
}