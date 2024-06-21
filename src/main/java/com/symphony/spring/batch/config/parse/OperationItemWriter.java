package com.symphony.spring.batch.config.parse;

import com.symphony.spring.batch.entity.CsvFile;
import com.symphony.spring.batch.entity.Operation;
import com.symphony.spring.batch.repository.FileRepository;
import com.symphony.spring.batch.repository.OperationRepository;
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
public class OperationItemWriter implements ItemWriter<Operation>, StepExecutionListener {

    private static final Logger log =
            LoggerFactory.getLogger(OperationItemWriter.class);

    private final OperationRepository operationRepository;
    private final FileRepository fileRepository;

    private StepExecution stepExecution;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        updateFileWithNewCount();
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends Operation> list) throws Exception {
        log.info("Operations writing | {} records .. ", list.size());
        operationRepository.saveAll(list);

        // todo: write a logic to track evolution of the parsing
        updateFileWithNewCount();

    }


    private void updateFileWithNewCount() {
        CsvFile csvFile = fileRepository.findById(stepExecution.getJobExecution().getJobParameters().getString("fileId")).get();
        csvFile.setProcessedLines(this.stepExecution.getReadCount());
        fileRepository.save(csvFile);
    }
}
