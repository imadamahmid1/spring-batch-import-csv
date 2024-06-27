package com.symphony.spring.batch.service;

import com.symphony.spring.batch.entity.CsvFile;
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
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

import static com.symphony.spring.batch.constant.DiskConstants.TEMP_STORAGE;

@Service
@RequiredArgsConstructor
public class ImportCsvAndParseJobService {
    private static final Logger log =
            LoggerFactory.getLogger(ImportCsvAndParseJobService.class);

    private final JobLauncher jobLauncher;
    private final FileRepository fileRepository;
    private final Job importJob;

    public CsvFile importCsvAndParseJobForCsvFile(MultipartFile multipartFile) throws IOException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
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
        return csvFileMetadata;
    }


}
