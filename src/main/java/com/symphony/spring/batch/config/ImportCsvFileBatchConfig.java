package com.symphony.spring.batch.config;

import com.symphony.spring.batch.config.parse.MakeParsingReportTasklet;
import com.symphony.spring.batch.config.parse.OperationItemValidator;
import com.symphony.spring.batch.config.parse.OperationItemWriter;
import com.symphony.spring.batch.entity.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class ImportCsvFileBatchConfig {

    private static final int CHUNK_SIZE = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final OperationItemWriter customerItemWriter;
    private final OperationItemValidator operationItemValidator;
    private final MakeParsingReportTasklet makeParsingReportTasklet;

    //
    // Csv -> Mongo
    //

    //todo: item reader validator extract it as a class next to the operation item validator and writer

    @Bean("csvItemReader")
    @StepScope
    public FlatFileItemReader<Operation> csvItemReader(@Value("#{jobParameters[fullPathFileName]}") String pathToFile, @Value("#{jobParameters}") Map<String, Object> jobParameters) {
        FlatFileItemReader<Operation> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(new File(pathToFile)));
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }


    private LineMapper<Operation> lineMapper() {
        DefaultLineMapper<Operation> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("advisorId", "operationType", "emp");

        BeanWrapperFieldSetMapper<Operation> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Operation.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

//    // todo: comment this and see if it wuuld work!
//    @Bean
//    @StepScope
//    public OperationItemValidator csvLineValidator() {
//        return new OperationItemValidator();
//    }


    @Bean
    // to become a scoped bean
    public Step readCsvFileAndSaveOperationsToDb(FlatFileItemReader<Operation> csvItemReader) {
        return stepBuilderFactory.get("parseCsvAndSaveOperationsToDatabase").<Operation, Operation>chunk(CHUNK_SIZE)
                .reader(csvItemReader)
                .processor(operationItemValidator)
                .writer(customerItemWriter)
                .build();
    }

    //
    // Reporting
    //

    @Bean
    // to become a scoped bean
    // delete all the Operation existing for a file
    public Step generateCsvImportingReport() {
        return stepBuilderFactory.get("generateAndSendParsingReport")
                .tasklet(makeParsingReportTasklet)
                .build();
    }


    @Bean
    @Qualifier("importJob")
    public Job importJob(FlatFileItemReader<Operation> itemReader) {
        return jobBuilderFactory.get("importCsvFile")
                .start(readCsvFileAndSaveOperationsToDb(itemReader))
                .next(generateCsvImportingReport())
                // todo: add a tasklet to delete files from disk (import file and the errors report file if any)
                .build();
    }

}
