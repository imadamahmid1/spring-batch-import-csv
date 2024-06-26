package com.symphony.spring.batch.service;

import com.symphony.spring.batch.entity.CsvFile;
import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import com.symphony.spring.batch.repository.FileRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ExecuteCsvFileJobService {
    private static final Logger log =
            LoggerFactory.getLogger(ExecuteCsvFileJobService.class);

    private final JobOperator jobOperator;
    private final JobLauncher jobLauncher;
    private final Job executeJob;
    private final JobExecutionDao jobExecutionDao;
    private final FileRepository fileRepository;
    private final CsvFileExecutionJobRepository csvExecutionJobRepository;

    /**
     * Validates that a fileId was successfully imported, if not throw error and end flow.
     * When the import and parsing is successful, we have two cases:
     * 1 - The 1st time we execute the operations of the file: create a new CsvFileExecutionJob in our database and a new Job Instance in spring batch
     * 2 - A 1st ExecuteJob was already run before: restart one of the ExecuteCsv Job Execution and return the Execution ID
     *
     * @param fileId
     * @return
     * @throws JobExecutionAlreadyRunningException
     * @throws JobRestartException
     * @throws JobInstanceAlreadyCompleteException
     * @throws JobParametersInvalidException
     * @throws NoSuchJobExecutionException
     * @throws NoSuchJobException
     */
    public CsvFileExecutionJob validateImportingOfFileAndRunExecuteCsvFileJob(String fileId) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, NoSuchJobExecutionException, NoSuchJobException {
        CsvFile csvFileMetadata = fileRepository.findById(fileId).get();
        validateFileWasSuccessfullyImportedAndParsed(csvFileMetadata);

        List<CsvFileExecutionJob> byFileId = csvExecutionJobRepository.findByFileId(fileId);
        CsvFileExecutionJob csvFileExecutionJob = createNewCsvFileExecutionJob(fileId);

        return csvFileExecutionJob;
    }


    /**
     * Create a new CsvFileExecutionJob model, and link it to the new Job Instance of type "executeJob"
     *
     * @param fileId
     * @return
     * @throws JobExecutionAlreadyRunningException
     * @throws JobRestartException
     * @throws JobInstanceAlreadyCompleteException
     * @throws JobParametersInvalidException
     */
    public CsvFileExecutionJob createNewCsvFileExecutionJob(String fileId) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
        CsvFileExecutionJob csvFileExecutionJob;
        csvFileExecutionJob = csvExecutionJobRepository.save(CsvFileExecutionJob.builder()
                .fileId(fileId)
                .isExecuted(false)
                .build());
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fileId", fileId)
                .addString("csvFileExecutionJobId", csvFileExecutionJob.getId())
                .addLong("startAt", System.currentTimeMillis()).toJobParameters();

        log.info("Running import CSV file job | fileId=[{}]", fileId);
        JobExecution importFileJob = jobLauncher.run(executeJob, jobParameters);

        csvFileExecutionJob.setJobInstanceId(importFileJob.getJobInstance().getInstanceId());
        csvExecutionJobRepository.save(csvFileExecutionJob);
        return csvFileExecutionJob;
    }

    public CsvFileExecutionJob restartExecutionJobInstance(Long jobInstanceId) throws JobInstanceAlreadyCompleteException, NoSuchJobExecutionException, NoSuchJobException, JobRestartException, JobParametersInvalidException, JobExecutionAlreadyRunningException {
        List<CsvFileExecutionJob> byJobInstanceId = csvExecutionJobRepository.findByJobInstanceId(jobInstanceId);


        return validateImportingOfFileAndRunExecuteCsvFileJob(byJobInstanceId.get(0).getFileId());
    }

    public Map<String, Boolean> stopExecutionJobExecutions(Long jobInstanceId) {
        List<JobExecution> jobExecutions = jobExecutionDao.findJobExecutions(new JobInstance(jobInstanceId, "" + jobInstanceId));
        Map<String, Boolean> jobMap = new HashMap<>();
        jobExecutions.stream().forEach(job -> {
            try {
                // In order to make sure the job is abandoned because if you try to stop a job multiple times it might fail
                //  we can also add: jobOperator.abandon(jobExecutions.get(0).getId());

                jobMap.put("isStopped", jobOperator.stop(job.getId()));
            } catch (Exception e) {
                log.error("Stopping Job error | jobInstanceId=[{}]", jobInstanceId);
                jobMap.put("isStopped", false);
            }
        });

        return jobMap;
    }


    private void validateFileWasSuccessfullyImportedAndParsed(CsvFile csvFileMetadata) {
        if (csvFileMetadata.getHasParsingErrors()) {
            throw new IllegalArgumentException("File not parsed successfully and has parsing errors!");
        }
        if (!csvFileMetadata.getIsParsed()) {
            throw new IllegalArgumentException("File import and parsing is not finalized yet!");
        }
    }


}
