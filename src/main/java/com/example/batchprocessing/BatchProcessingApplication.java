package com.example.batchprocessing;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.content.solr.SolrProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
public class BatchProcessingApplication {

    public static void main(String[] args) throws Exception {
        System.exit(SpringApplication.exit(SpringApplication.run(BatchProcessingApplication.class, args)));
    }

    @Configuration
    public static class Config {

        @Bean
        public SolrClient solrClient(SolrProperties props) {
            props.setUser("solr");
            props.setPassword("SolrRocks");
            return new HttpSolrClient.Builder(props.getUrl()).build();
        }
    }
}
