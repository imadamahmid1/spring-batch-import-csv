package com.symphony.spring.batch.repository;

import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CsvFileExecutionJobRepository extends MongoRepository<CsvFileExecutionJob, String> {

    List<CsvFileExecutionJob> findByFileId(String fileId);

}
