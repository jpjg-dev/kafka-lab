package com.jipi.producerservice.message;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StudyMessageRepository extends JpaRepository<StudyMessage, Long> {
}
