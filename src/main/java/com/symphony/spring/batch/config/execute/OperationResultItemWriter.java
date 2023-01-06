package com.symphony.spring.batch.config.execute;

import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.entity.OperationResult;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import com.symphony.spring.batch.repository.OperationResultRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@StepScope
@RequiredArgsConstructor
public class OperationResultItemWriter implements ItemWriter<OperationResult>, StepExecutionListener {

    private static final Logger log =
            LoggerFactory.getLogger(OperationResultItemWriter.class);

    private final OperationResultRepository operationResultRepository;
    private final CsvFileExecutionJobRepository csvFileExecutionJobRepository;

    private StepExecution stepExecution;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        updateCsvFileJobWithNewEntitledCount();
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends OperationResult> list) throws Exception {
        log.info("Operations results writing | {} records .. ", list.size());
        operationResultRepository.saveAll(list);

        // todo: write a logic to track evolution of the entitling
        updateCsvFileJobWithNewEntitledCount();
    }


    private void updateCsvFileJobWithNewEntitledCount() {
        CsvFileExecutionJob csvFileExecutionJob = csvFileExecutionJobRepository
                .findById(stepExecution.getJobExecution().getJobParameters().getString("csvFileExecutionJobId")).get();

        csvFileExecutionJob.setProcessedOperations(this.stepExecution.getReadCount());
        csvFileExecutionJobRepository.save(csvFileExecutionJob);
    }

}
