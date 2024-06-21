package com.symphony.spring.batch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.GeneratedValue;
import java.util.List;

@Document("FederationOperation")
@Data
@AllArgsConstructor
@NoArgsConstructor
// todo: add indexes on the file Id
public class Operation {
    @Id
    @GeneratedValue(generator = "system-uuid")
    @GenericGenerator(name = "system-uuid", strategy = "uuid")
    private String id;

    private String fileId;

    private String advisorId;
    // I removed the usage of the Enum in order to avoid having an error in the spring batch reader side instead of having it in the process side

    private String operationType;
    private String emp;
    private List<String> errors;
    private boolean hasError;

}
