package com.symphony.spring.batch.config.parse;

import com.symphony.spring.batch.entity.CsvFile;
import com.symphony.spring.batch.entity.Operation;
import com.symphony.spring.batch.repository.FileRepository;
import com.symphony.spring.batch.repository.OperationRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.symphony.spring.batch.constant.DiskConstants.TEMP_STORAGE;

@Component
@StepScope
@RequiredArgsConstructor
public class MakeParsingReportTasklet implements Tasklet {

    private static final Logger log =
            LoggerFactory.getLogger(MakeParsingReportTasklet.class);


    private final OperationRepository operationRepository;
    private final FileRepository fileRepository;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        String fileId = (String) chunkContext.getStepContext().getJobParameters().get("fileId");

        log.warn("Starting collection parsing results ... ");

        List<Operation> allByFileId = operationRepository.findByFileId(fileId);

        boolean hasErrors = allByFileId.stream().anyMatch(op -> op.isHasError());

        generateReport(allByFileId, fileId);
        if (hasErrors) {
            handleErrors(allByFileId, fileId);
        } else {
            handleSuccess(fileId);
        }
        log.warn("End collection parsing results ... ");
        return null;
    }

    private void handleErrors(List<Operation> allByFileId, String fileId) {
        Optional<CsvFile> byId = fileRepository.findById(fileId);
        byId.get().setIsParsed(true);
        byId.get().setHasParsingErrors(true);
        fileRepository.save(byId.get());

        log.info("++++++++++++++++++++++++");
        log.error("Failure in Parsing Job!");
        log.error("Failed {}",  allByFileId.stream().filter(op -> op.isHasError()).collect(Collectors.toList()));
        log.info("++++++++++++++++++++++++");
        operationRepository.deleteByFileId(fileId);
    }

    private void handleSuccess(String fileId) {
        Optional<CsvFile> byId = fileRepository.findById(fileId);
        byId.get().setIsParsed(true);
        byId.get().setHasParsingErrors(false);
        fileRepository.save(byId.get());

        log.info("+++++++++++++++++++++++++++++");
        log.info("Csv file {} parsed successfully!");
        log.info("+++++++++++++++++++++++++++++");
    }


    private static void generateReport(List<Operation> results, String csvFileId) {
        try {
            String csvData = convertToCSV(results);
            writeCSVToFile(csvData, csvFileId);
        } catch (IOException e) {
            log.error("Error happened while writing in CSV file");
        }
    }

    private static String convertToCSV(List<Operation> results) {
        StringBuilder csv = new StringBuilder();

        // Header row
        csv.append("status,advisorId,operationType,emp\n");

        for (Operation res : results) {
            csv
                    .append(res.isHasError() ? "FAILED" : "SUCCESS").append(",")
                    .append(res.getAdvisorId()).append(",")
                    .append(res.getOperationType()).append(",")
                    .append(res.getEmp());
            if (res.isHasError()) {
                csv.append(",").append(res.getErrors());
            }

            csv.append("\n");
        }

        return csv.toString();
    }

    private static void writeCSVToFile(String csvData, String csvFileExecutionJobId) throws IOException {
        String filePath = TEMP_STORAGE + csvFileExecutionJobId + "_parse_results.csv";
        File f = new File(filePath);
        FileOutputStream outStr = new FileOutputStream(filePath, false);

        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        writer.write(csvData);
        writer.close();
    }

}
