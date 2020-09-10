package com.example.batchprocessing;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort.Direction;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private AccountStateRepository repository;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public RepositoryItemReader<AccountState> reader() {

        Map<String, Direction> sortMap = new HashMap<>();
        sortMap.put("id", Direction.DESC);

        return new RepositoryItemReaderBuilder<AccountState>()
                .repository(repository)
                .methodName("findAll")
                .sorts(sortMap)
                .saveState(false)
                .build();
    }

    @Bean
    public ItemWriter writer() {
        return new AccountStateReindexingWriter();
    }

    @Bean
    public Job reIndexContentJob(Step reindex) {
        return jobBuilderFactory.get("reIndexContent")
                .incrementer(new RunIdIncrementer())
                .flow(reindex)
                .end()
                .build();
    }

    @Bean
    public Step reindex() {
        return stepBuilderFactory.get("reindex")
                .<AccountState, AccountState> chunk(5)
                .reader(reader())
                .writer(writer())
                .build();
    }
}
