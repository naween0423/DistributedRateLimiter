package com.navbon.ratelimiter;

import com.navbon.ratelimiter.model.RecordItem;
import com.navbon.ratelimiter.producer.RecordProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaRateLimiterApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaRateLimiterApplication.class, args);
    }

    @Bean
    public CommandLineRunner testProducer(RecordProducer producer) {
        return args -> {
            System.out.println("--- Seeding Kafka with 25 test records ---");
            for (int i = 1; i <= 200; i++) {
                RecordItem item = new RecordItem("REC-" + i, "Payload data #" + i, "PENDING", 0);
                producer.publishRecord(item);
                Thread.sleep(100);
            }
        };
    }
}
