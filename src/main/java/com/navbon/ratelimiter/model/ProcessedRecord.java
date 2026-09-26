package com.navbon.ratelimiter.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "processed_records")
public class ProcessedRecord {

    @Id
    private String recordId;
    private String payload;
    private String status;
    private int retryCount;

    protected ProcessedRecord() {
    }

    public ProcessedRecord(RecordItem record) {
        this.recordId = record.getRecordId();
        this.payload = record.getPayload();
        this.status = record.getStatus();
        this.retryCount = record.getRetryCount();
    }

}
