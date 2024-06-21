package com.symphony.spring.batch.config.execute;

import com.symphony.spring.batch.entity.Operation;
import com.symphony.spring.batch.entity.OperationResult;
import com.symphony.spring.batch.model.OperationType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@StepScope
@RequiredArgsConstructor
public class ExecuteOperationItemProcessor implements ItemProcessor<Operation, OperationResult> {
    private static final Logger log =
            LoggerFactory.getLogger(MakeExecutionReportTasklet.class);


    @Value("#{jobParameters['csvFileExecutionJobId']}")
    public String csvFileExecutionJobId;

    @Value("#{jobParameters}")
    public Map<String, Object> jobParameters;

    @Override
    public OperationResult process(Operation op) {
        OperationResult result = OperationResult.builder()
                .advisorId(op.getAdvisorId())
                .emp(op.getEmp())
                .fileExecutionJobId(csvFileExecutionJobId)
                .operationType(OperationType.valueOf(op.getOperationType()))
                .build();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (op.getAdvisorId().contains("fail")) {
            log.error("Failed entitling operation | advisorId=[{}] operationId=[{}]", op.getAdvisorId(), op.getId());
            result.setHasExecutionErrors(true);
            result.setExecutionErrors(List.of("This operation has failed for : " + op.getAdvisorId()));
        } else {
            log.info("Successful entitling operation | advisorId=[{}] operationId=[{}]", op.getAdvisorId(), op.getId());
        }

        return result;
    }

}
