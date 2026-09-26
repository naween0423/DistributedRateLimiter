package com.navbon.ratelimiter.service;

import com.navbon.ratelimiter.model.RecordItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ExternalApiService {

    private static final Logger log = LoggerFactory.getLogger(ExternalApiService.class);

    public boolean callExternalEndpoint(RecordItem record) {
        log.info("--> Calling External API for record ID: {}", record.getRecordId());
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }
}
