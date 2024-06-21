package com.symphony.spring.batch.config;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import eu.europeana.batch.config.MongoBatchConfigurer;
import eu.europeana.batch.entity.PackageMapper;
import eu.europeana.batch.repository.JobExecutionRepository;
import eu.europeana.batch.repository.JobInstanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;


/**
 * The goal of this conf class is to set the datastore and implement the MongoBatchConfigurer (that implements the Doa for the spring batch JOB schema
 */
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class MongoBatchConfigurerConfig {

    private final MongoClient mongoClient;

    public Datastore batchDatastore() {
        Datastore datastore = Morphia.createDatastore(mongoClient, "importCSV");

        // Required to create indices on database
        datastore.getMapper().mapPackage(PackageMapper.class.getPackageName());
        datastore.ensureIndexes();
        return datastore;
    }

    @Bean
    public MongoBatchConfigurer mongoBatchConfigurer() {
        return new MongoBatchConfigurer(batchDatastore(), new SimpleAsyncTaskExecutor());
    }

    @Bean
    public JobExecutionDao jobExecutionDao() {
        return new JobExecutionRepository(batchDatastore());
    }

    @Bean
    public JobInstanceDao jobInstanceDao() {
        return new JobInstanceRepository(batchDatastore());
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }

}
