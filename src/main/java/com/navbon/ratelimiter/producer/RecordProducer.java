package com.navbon.ratelimiter.producer;

import com.navbon.ratelimiter.model.RecordItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RecordProducer {

    private static final Logger log = LoggerFactory.getLogger(RecordProducer.class);
    private static final String TOPIC_RECORDS = "records-to-process";

    private final KafkaTemplate<String, RecordItem> kafkaTemplate;

    public RecordProducer(KafkaTemplate<String, RecordItem> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishRecord(RecordItem record) {
        log.info("Publishing record to Kafka topic [{}]: ID={}", TOPIC_RECORDS, record.getRecordId());
        kafkaTemplate.send(TOPIC_RECORDS, record.getRecordId(), record);
    }
}
