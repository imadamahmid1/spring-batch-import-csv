package com.symphony.spring.batch.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.GeneratedValue;

@Document("FederationCsvFile")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CsvFile {
    @Id
    @GeneratedValue(generator = "system-uuid")
    @GenericGenerator(name = "system-uuid", strategy = "uuid")
    private String id;

    private String fileName;
    private Long jobInstanceId;
    private Boolean isParsed;
    private Boolean hasParsingErrors;
    private int processedLines;
}
