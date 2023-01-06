package com.symphony.spring.batch.config;

import com.symphony.spring.batch.config.execute.MakeExecutionReportTasklet;
import com.symphony.spring.batch.config.execute.ExecuteOperationItemProcessor;
import com.symphony.spring.batch.config.execute.OperationResultItemWriter;
import com.symphony.spring.batch.entity.Operation;
import com.symphony.spring.batch.entity.OperationResult;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class ExecuteCsvFileBatchConfig {

    // todo: align the way i m importing stuff (Bean or through injections .. )
    private final JobBuilderFactory jobBuilderFactory;
    private final MongoTemplate mongoTemplate;
    private final StepBuilderFactory stepBuilderFactory;
    private final MakeExecutionReportTasklet makeExecutionReportTasklet;
    private final OperationResultItemWriter operationResultItemWriter;
    private final ExecuteOperationItemProcessor executeOperationItemProcessor;

    @Bean("mongoOperationReader")
    @StepScope
    public MongoItemReader<Operation> mongoOperationReader(@Value("#{jobParameters}") Map<String, Object> jobParameters) {
        MongoItemReader<Operation> reader = new MongoItemReader<>();

        Query query = new Query();
        query.addCriteria(Criteria.where("fileId").is(jobParameters.get("fileId")));
        query.noCursorTimeout();
        query.cursorBatchSize(1);

        reader.setTemplate(mongoTemplate);
        reader.setTargetType(Operation.class);
        reader.setQuery(query);

        reader.setSaveState(false);

        return reader;
    }

    @Bean
    public Step readOperationsFromDBAndExecute(MongoItemReader<Operation> mongoOperationReader) {
        return stepBuilderFactory.get("executeCsvOperationAndSendReport").<Operation, OperationResult>chunk(1)
                .reader(mongoOperationReader)
                .processor(executeOperationItemProcessor)
                .writer(operationResultItemWriter)
                .build();
    }

    //
    // Reporting
    //
    @Bean
    // to become a scoped bean
    // delete all the Operation existing for a file
    public Step generateCsvExecutionReport() {
        return stepBuilderFactory.get("generateAndSendExecutionResultsReport")
                .tasklet(makeExecutionReportTasklet)
                .build();
    }


    @Bean
    @Qualifier("executeJob")
    public Job executeJob(MongoItemReader<Operation> mongoOperationReader) {
        return jobBuilderFactory.get("executeCsvFile")
                .start(readOperationsFromDBAndExecute(mongoOperationReader))
                .next(generateCsvExecutionReport())
                .build();
    }

}
