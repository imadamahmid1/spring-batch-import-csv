package com.symphony.spring.batch.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.symphony.spring.batch.entity.Operation;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class StepSkipListener implements SkipListener<Operation, Number> {


    Logger logger = LoggerFactory.getLogger(StepSkipListener.class);

    @Override // item reader
    public void onSkipInRead(Throwable throwable) {
        logger.info("A failure on read {} ", throwable.getMessage());
    }

    @Override // item writter
    public void onSkipInWrite(Number item, Throwable throwable) {
        logger.info("A failure on write {} , {}", throwable.getMessage(), item);
    }

    @SneakyThrows
    @Override // item processor
    public void onSkipInProcess(Operation customer, Throwable throwable) {
        logger.info("Item {}  was skipped due to the exception  {}", new ObjectMapper().writeValueAsString(customer),
                throwable.getMessage());
    }
}
