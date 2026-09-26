package com.navbon.ratelimiter.repository;

import com.navbon.ratelimiter.model.ProcessedRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedRecordRepository extends JpaRepository<ProcessedRecord, String> {
}
