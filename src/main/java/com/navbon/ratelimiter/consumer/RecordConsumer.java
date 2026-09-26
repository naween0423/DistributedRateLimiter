package com.navbon.ratelimiter.consumer;

import com.navbon.ratelimiter.model.RecordItem;
import com.navbon.ratelimiter.model.ProcessedRecord;
import com.navbon.ratelimiter.repository.ProcessedRecordRepository;
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
    private final ProcessedRecordRepository processedRecordRepository;

    public RecordConsumer(RateLimiterService rateLimiterService,
                          ExternalApiService externalApiService,
                          KafkaTemplate<String, RecordItem> kafkaTemplate,
                          ProcessedRecordRepository processedRecordRepository) {
        this.rateLimiterService = rateLimiterService;
        this.externalApiService = externalApiService;
        this.kafkaTemplate = kafkaTemplate;
        this.processedRecordRepository = processedRecordRepository;
    }

    @KafkaListener(topics = INPUT_TOPIC, groupId = "external-api-workers")
    public void consume(RecordItem record) {
        log.info("Received record from Kafka: {}", record.getRecordId());
        processedRecordRepository.save(new ProcessedRecord(record));

        while (!rateLimiterService.tryConsume()) {
            log.warn("Rate limit reached (100 calls/min). Consumer backing off for 500ms...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        boolean success;
        try {
            success = externalApiService.callExternalEndpoint(record);
        } catch (Exception e) {
            log.error("Exception invoking API for record: {}", record.getRecordId(), e);
            handleFailure(record);
            return;
        }

        if (success) {
            record.setStatus("COMPLETED");
            saveRecord(record);
            log.info("Successfully processed record: {}", record.getRecordId());
        } else {
            handleFailure(record);
        }
    }

    private void handleFailure(RecordItem record) {
        int retries = record.getRetryCount() + 1;
        record.setRetryCount(retries);

        if (retries >= MAX_RETRIES) {
            record.setStatus("FAILED_DLQ");
            saveRecord(record);
            log.error("Max retries exceeded for record {}. Moving to DLQ.", record.getRecordId());
            kafkaTemplate.send(DLQ_TOPIC, record.getRecordId(), record);
        } else {
            record.setStatus("PENDING_RETRY");
            saveRecord(record);
            log.warn("Retrying record {}, attempt #{}", record.getRecordId(), retries);
            kafkaTemplate.send(INPUT_TOPIC, record.getRecordId(), record);
        }
    }

    private void saveRecord(RecordItem record) {
        processedRecordRepository.save(new ProcessedRecord(record));
    }
}
