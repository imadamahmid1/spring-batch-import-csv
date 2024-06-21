package com.symphony.spring.batch.config.parse;

import com.symphony.spring.batch.entity.Operation;
import com.symphony.spring.batch.model.OperationType;
import io.micrometer.core.instrument.util.StringUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
@StepScope
@RequiredArgsConstructor
public class OperationItemValidator implements ItemProcessor<Operation, Operation> {

    @Value("#{jobParameters['fileId']}")
    public String fileId;

    @Value("#{jobParameters}")
    public Map<String, Object> jobParameters;

    @Override
    public Operation process(Operation operation) {
        operation.setFileId(fileId);
        List<String> errors = new ArrayList<>();
        if (StringUtils.isEmpty(operation.getEmp())) {
            errors.add("Emp is null");
            operation.setHasError(true);
        }


        boolean hasValidOperationType = Arrays.stream(OperationType.values()).anyMatch((t) -> t.name().equals(operation.getOperationType()));
        if (!hasValidOperationType) {
            errors.add("Invalid Operation type");
            operation.setHasError(true);
        }
        operation.setErrors(errors);
        return operation;
    }
}
