package com.symphony.spring.batch.controller;

import com.symphony.spring.batch.entity.CsvFile;
import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import com.symphony.spring.batch.repository.FileRepository;
import com.symphony.spring.batch.service.ExecuteCsvFileJobService;
import com.symphony.spring.batch.service.ImportCsvAndParseJobService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class CsvFileController {
    private static final Logger log =
            LoggerFactory.getLogger(CsvFileController.class);

    private final FileRepository fileRepository;
    private final CsvFileExecutionJobRepository csvExecutionJobRepository;
    private final ExecuteCsvFileJobService executeCsvFileJobService;
    private final ImportCsvAndParseJobService importCsvAndParseJobService;

    @PostMapping(path = "/importCsvFile")
    public ResponseEntity<CsvFile> importCsvFile(@RequestParam("file") MultipartFile multipartFile) throws IOException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        log.info("Import new CSV file | name=[{}]", multipartFile.getOriginalFilename());
        CsvFile csvFileMetadata = importCsvAndParseJobService.importCsvAndParseJobForCsvFile(multipartFile);

        return ResponseEntity.ok(csvFileMetadata);
    }



    @PostMapping("/file/{fileId}/execute")
    public ResponseEntity<CsvFileExecutionJob> executeCsvFileOperations(@PathVariable String fileId) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, NoSuchJobException, NoSuchJobExecutionException {
        CsvFileExecutionJob csvFileExecutionJob = executeCsvFileJobService.validateImportingOfFileAndRunExecuteCsvFileJob(fileId);
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
