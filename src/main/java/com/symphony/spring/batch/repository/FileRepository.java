package com.symphony.spring.batch.repository;

import com.symphony.spring.batch.entity.CsvFile;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileRepository extends MongoRepository<CsvFile, String> {
}
