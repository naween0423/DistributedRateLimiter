package com.navbon.ratelimiter.model;

import java.io.Serializable;

public class RecordItem implements Serializable {
    private String recordId;
    private String payload;
    private String status;
    private int retryCount;

    public RecordItem() {}

    public RecordItem(String recordId, String payload, String status, int retryCount) {
        this.recordId = recordId;
        this.payload = payload;
        this.status = status;
        this.retryCount = retryCount;
    }

    public String getRecordId() { return recordId; }
    public void setRecordId(String recordId) { this.recordId = recordId; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    @Override
    public String toString() {
        return "RecordItem{" +
                "recordId='" + recordId + '\'' +
                ", payload='" + payload + '\'' +
                ", status='" + status + '\'' +
                ", retryCount=" + retryCount +
                '}';
    }
}
