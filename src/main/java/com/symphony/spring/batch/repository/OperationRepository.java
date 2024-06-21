package com.symphony.spring.batch.repository;

import com.symphony.spring.batch.entity.Operation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OperationRepository extends MongoRepository<Operation, String> {

    List<Operation> findByFileId(String fileId);

    void deleteByFileId(String fileId);

}
