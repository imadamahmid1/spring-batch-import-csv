package com.symphony.spring.batch.repository;

import com.symphony.spring.batch.entity.OperationResult;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OperationResultRepository extends MongoRepository<OperationResult, String> {

    // The job Id is our execution job id in our Mongo model (Not the one from Spring Batch)
    List<OperationResult> findByFileExecutionJobId(String jobId);

    void deleteByFileExecutionJobId(String jobId);

}
