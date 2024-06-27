package com.symphony.spring.batch.controller;

import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.service.ExecuteCsvFileJobService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.data.util.Pair;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class CsvFileExecutionJobController {
    private static final Logger log =
            LoggerFactory.getLogger(CsvFileExecutionJobController.class);

    private final ExecuteCsvFileJobService executeCsvFileJobService;


    @PostMapping(path = "/job/{jobInstanceId}/stop")
    public ResponseEntity<Map<String, Boolean>> stopJob(@PathVariable Long jobInstanceId) {
        log.info("Stopping job executions for job instance Id=[{}]", jobInstanceId);
        return ResponseEntity.ok(executeCsvFileJobService.stopExecutionJobExecutions(jobInstanceId));
    }


    @PostMapping(path = "/job/{jobInstanceId}/restart")
    public ResponseEntity<CsvFileExecutionJob> restartJob(@PathVariable Long jobInstanceId) throws NoSuchJobExecutionException, JobInstanceAlreadyCompleteException, NoSuchJobException, JobParametersInvalidException, JobRestartException, JobExecutionAlreadyRunningException {
        log.info("Restarting a new execution for job instance Id=[{}]", jobInstanceId);

        CsvFileExecutionJob csvFileExecutionJob = executeCsvFileJobService.restartExecutionJobInstance(jobInstanceId);
        return ResponseEntity.ok(csvFileExecutionJob);
    }
}
