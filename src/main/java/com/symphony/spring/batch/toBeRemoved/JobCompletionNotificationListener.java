package com.symphony.spring.batch.toBeRemoved;


import com.symphony.spring.batch.repository.FileRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
// todo: delete this class
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private final FileRepository fileRepository;
    private static final Logger log =
            LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
//            fileRepository.findById(jobExecution.getExecutionContext().)
            log.info("!!! JOB FINISHED! Notify user with successful parsing of his CSV file ID: ", jobExecution.getJobId());
        }
    }
}
