package com.navbon.ratelimiter.consumer;

import com.navbon.ratelimiter.model.RecordItem;
import com.navbon.ratelimiter.service.ExternalApiService;
import com.navbon.ratelimiter.service.RateLimiterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(RecordConsumer.class);
    private static final String DLQ_TOPIC = "records-dlq";
    private static final String INPUT_TOPIC = "records-to-process";
    private static final int MAX_RETRIES = 3;

    private final RateLimiterService rateLimiterService;
    private final ExternalApiService externalApiService;
    private final KafkaTemplate<String, RecordItem> kafkaTemplate;

    public RecordConsumer(RateLimiterService rateLimiterService,
                          ExternalApiService externalApiService,
                          KafkaTemplate<String, RecordItem> kafkaTemplate) {
        this.rateLimiterService = rateLimiterService;
        this.externalApiService = externalApiService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = INPUT_TOPIC, groupId = "external-api-workers")
    public void consume(RecordItem record) {
        log.info("Received record from Kafka: {}", record.getRecordId());

        while (!rateLimiterService.tryConsume()) {
            log.warn("Rate limit reached (100 calls/min). Consumer backing off for 500ms...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        try {
            boolean success = externalApiService.callExternalEndpoint(record);
            if (success) {
                record.setStatus("COMPLETED");
                log.info("Successfully processed record: {}", record.getRecordId());
            } else {
                handleFailure(record);
            }
        } catch (Exception e) {
            log.error("Exception invoking API for record: {}", record.getRecordId(), e);
            handleFailure(record);
        }
    }

    private void handleFailure(RecordItem record) {
        int retries = record.getRetryCount() + 1;
        record.setRetryCount(retries);

        if (retries >= MAX_RETRIES) {
            record.setStatus("FAILED_DLQ");
            log.error("Max retries exceeded for record {}. Moving to DLQ.", record.getRecordId());
            kafkaTemplate.send(DLQ_TOPIC, record.getRecordId(), record);
        } else {
            record.setStatus("PENDING_RETRY");
            log.warn("Retrying record {}, attempt #{}", record.getRecordId(), retries);
            kafkaTemplate.send(INPUT_TOPIC, record.getRecordId(), record);
        }
    }
}
