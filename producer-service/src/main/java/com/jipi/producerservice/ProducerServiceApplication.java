package com.jipi.producerservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ProducerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerServiceApplication.class, args);
    }

}
