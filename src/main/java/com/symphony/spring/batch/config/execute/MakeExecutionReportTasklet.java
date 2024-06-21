package com.symphony.spring.batch.config.execute;

import com.symphony.spring.batch.entity.CsvFileExecutionJob;
import com.symphony.spring.batch.entity.OperationResult;
import com.symphony.spring.batch.repository.CsvFileExecutionJobRepository;
import com.symphony.spring.batch.repository.OperationResultRepository;
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

import static com.symphony.spring.batch.constant.DiskConstants.TEMP_STORAGE;

@Component
@StepScope
@RequiredArgsConstructor
public class MakeExecutionReportTasklet implements Tasklet {

    private static final Logger log =
            LoggerFactory.getLogger(MakeExecutionReportTasklet.class);


    private final OperationResultRepository operationResultRepository;
    private final CsvFileExecutionJobRepository csvFileExecutionJobRepository;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        String csvExecutionJobId = (String) chunkContext.getStepContext().getJobParameters().get("csvFileExecutionJobId");

        sendReport(csvExecutionJobId);

        return null;
    }

    private static void generateReport(List<OperationResult> results, String csvFileExecutionJobId) {
        try {
            String csvData = convertToCSV(results);
            writeCSVToFile(csvData, csvFileExecutionJobId);
        } catch (IOException e) {
            log.error("Error happened while writing in CSV file");
        }
    }

    private void sendReport(String csvFileExecutionJobId) {
        List<OperationResult> allByFileId = operationResultRepository.findByFileExecutionJobId(csvFileExecutionJobId);

        log.info("Making CSV result file to be loaded in S3 ...!");
        generateReport(allByFileId, csvFileExecutionJobId);

        log.info("Loading CSV file to S3 ... ");

        CsvFileExecutionJob byId = csvFileExecutionJobRepository.findById(csvFileExecutionJobId).get();
        byId.setLinkToCsv("https://s3.com/dummy-report.csv");
        byId.setIsExecuted(true);

        boolean hasErrors = allByFileId.stream().anyMatch(op -> op.isHasExecutionErrors());

        byId.setHasExecutionErrors(hasErrors);

        csvFileExecutionJobRepository.save(byId);

        // Delete all operation results after making the report to clean up the database
        operationResultRepository.deleteByFileExecutionJobId(byId.getFileId());

        log.info("+++++++++++++++++++++++++++++");
        log.info("Csv file {} executed successfully!", byId.getFileId());
        log.info("+++++++++++++++++++++++++++++");
    }

    private static String convertToCSV(List<OperationResult> results) {
        StringBuilder csv = new StringBuilder();

        // Header row
        csv.append("status,advisorId,operationType,emp\n");

        for (OperationResult res : results) {
            csv
                    .append(res.isHasExecutionErrors() ? "FAILED" : "SUCCESS").append(",")
                    .append(res.getAdvisorId()).append(",")
                    .append(res.getOperationType()).append(",")
                    .append(res.getEmp());
            if (res.isHasExecutionErrors()) {
                csv.append(",").append(res.getExecutionErrors());
            }

            csv.append("\n");
        }

        return csv.toString();
    }

    private static void writeCSVToFile(String csvData, String csvFileExecutionJobId) throws IOException {
        String filePath = TEMP_STORAGE + csvFileExecutionJobId + "_exec_results.csv";
        File f = new File(filePath);
        FileOutputStream outStr = new FileOutputStream(filePath, false);

        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        writer.write(csvData);
        writer.close();
    }


}
