package com.symphony.spring.batch.controller;

import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class CsvFileExecutionJobController {
    private static final Logger log =
            LoggerFactory.getLogger(CsvFileExecutionJobController.class);

    private final JobOperator jobOperator;
    private final JobExecutionDao jobExecutionDao;
    private final CsvFileExecutionJobRepository csvFileExecutionJobRepository;


    @PostMapping(path = "/job/{jobInstanceId}/stop")
    public ResponseEntity<Boolean> stopJob(@PathVariable Long jobInstanceId) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobExecutionAlreadyRunningException {
        List<JobExecution> jobExecutions = jobExecutionDao.findJobExecutions(new JobInstance(jobInstanceId, ".."));
        boolean stop = jobOperator.stop(jobExecutions.get(0).getId());

        // In order to make sure the job is abandoned because if you try to stop a job multiple times it might fail
        // jobOperator.abandon(jobExecutions.get(0).getId());

        return ResponseEntity.ok(stop);
    }

    @PostMapping(path = "/job/{jobInstanceId}/restart")
    public ResponseEntity<Long> restartJob(@PathVariable Long jobInstanceId) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobInstanceAlreadyCompleteException, NoSuchJobException, JobParametersInvalidException, JobRestartException {
        List<CsvFileExecutionJob> jobs = csvFileExecutionJobRepository.findByJobInstanceId(jobInstanceId);
        List<JobExecution> jobExecutions = jobExecutionDao.findJobExecutions(new JobInstance(jobInstanceId, ".."));
        Long newJobInstanceId = jobOperator.restart(jobExecutions.get(0).getId());

        return ResponseEntity.ok(newJobInstanceId);
    }

}
