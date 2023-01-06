package com.symphony.spring.batch.entity;

import com.symphony.spring.batch.model.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.GeneratedValue;
import java.util.List;

@Document("FederationOperationResult")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OperationResult {
    @Id
    @GeneratedValue(generator = "system-uuid")
    @GenericGenerator(name = "system-uuid", strategy = "uuid")
    private String id;
    private String fileId;
    private String fileExecutionJobId;

    private String advisorId;
    private OperationType operationType;
    private String emp;

    private List<String> executionErrors;
    private boolean hasExecutionErrors;
}
