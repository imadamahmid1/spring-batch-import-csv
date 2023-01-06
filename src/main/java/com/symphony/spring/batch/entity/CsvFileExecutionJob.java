package com.symphony.spring.batch.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.GeneratedValue;

@Document("FederationCsvFileExecution")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CsvFileExecutionJob {
    @Id
    @GeneratedValue(generator = "system-uuid")
    @GenericGenerator(name = "system-uuid", strategy = "uuid")
    private String id;
    private String fileId;

    private Long jobInstanceId;

    private String linkToCsv;
    private Boolean isExecuted;
    private Boolean hasExecutionErrors;

    private int processedOperations;
}
