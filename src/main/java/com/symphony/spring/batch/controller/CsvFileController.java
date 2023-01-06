package com.symphony.spring.batch.controller;

import com.symphony.spring.batch.entity.CsvFile;
import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import com.symphony.spring.batch.repository.FileRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.symphony.spring.batch.constant.DiskConstants.TEMP_STORAGE;

@RestController
@RequiredArgsConstructor
public class CsvFileController {
    private static final Logger log =
            LoggerFactory.getLogger(CsvFileController.class);

    private final JobLauncher jobLauncher;
    private final Job importJob;
    private final Job executeJob;
    private final FileRepository fileRepository;
    private final CsvFileExecutionJobRepository csvExecutionJobRepository;

    @PostMapping(path = "/importCsvFile")
    public ResponseEntity<CsvFile> startBatch(@RequestParam("file") MultipartFile multipartFile) throws IOException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        CsvFile csvFileMetadata = fileRepository.save(CsvFile.builder().fileName(multipartFile.getOriginalFilename()).isParsed(false).build());

        String filePath = TEMP_STORAGE + csvFileMetadata.getId() + ".csv";
        File fileToImport = new File(filePath);
        multipartFile.transferTo(fileToImport);


        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fullPathFileName", filePath)
                .addString("fileId", csvFileMetadata.getId())
                .addLong("startAt", System.currentTimeMillis()).toJobParameters();

        log.info("Running import CSV file job...");
        JobExecution importFileJob = jobLauncher.run(importJob, jobParameters);

        csvFileMetadata.setJobInstanceId(importFileJob.getJobInstance().getInstanceId());
        fileRepository.save(csvFileMetadata);

        return ResponseEntity.ok(csvFileMetadata);
    }


    @PostMapping("/file/{fileId}/execute")
    public ResponseEntity<CsvFileExecutionJob> executeCsvFileOperations(@PathVariable String fileId) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        CsvFile csvFileMetadata = fileRepository.findById(fileId).get();
        CsvFileExecutionJob csvFileExecutionJob =  csvExecutionJobRepository.save(CsvFileExecutionJob.builder()
                .fileId(fileId)
                .build());

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fileId", csvFileMetadata.getId())
                .addString("csvFileExecutionJobId", csvFileExecutionJob.getId())
                .addLong("startAt", System.currentTimeMillis()).toJobParameters();

        log.info("Running import CSV file job...");
        JobExecution importFileJob = jobLauncher.run(executeJob, jobParameters);

        csvFileExecutionJob.setJobInstanceId(importFileJob.getJobInstance().getInstanceId());
        csvExecutionJobRepository.save(csvFileExecutionJob);

        return ResponseEntity.ok(csvFileExecutionJob);
    }


    @GetMapping("/files")
    public List<CsvFile> getAll() {
        List<CsvFile> allFiles = fileRepository.findAll();
        // add all job instances of spring batch
        // add all File Execution Jobs with our repository

        return allFiles;
    }

    @GetMapping("/file/{fileId}")
    public ResponseEntity getFile(@PathVariable String fileId) {
        CsvFile file = fileRepository.findById(fileId).get();
        List<CsvFileExecutionJob> allJobsForFile = csvExecutionJobRepository.findByFileId(fileId);
        // add all job instances of spring batch
        // add all File Execution Jobs with our repository
        // add all CSV execution Jobs instances of Spring batch

        return ResponseEntity.ok(Map.of("fileInfo", file, "executionJobs", allJobsForFile));
    }
}
