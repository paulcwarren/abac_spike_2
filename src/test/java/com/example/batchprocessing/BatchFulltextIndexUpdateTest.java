package com.example.batchprocessing;

import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.AfterEach;
import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.BeforeEach;
import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.Context;
import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.Describe;
import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.It;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.content.solr.AttributeProvider;
import org.springframework.content.solr.SolrProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import com.github.paulcwarren.ginkgo4j.Ginkgo4jConfiguration;
import com.github.paulcwarren.ginkgo4j.Ginkgo4jSpringRunner;

import internal.org.springframework.content.solr.SolrFulltextIndexServiceImpl;

@RunWith(Ginkgo4jSpringRunner.class)
@Ginkgo4jConfiguration(threads = 1)
@SpringBatchTest
@EnableAutoConfiguration
@ContextConfiguration(classes = { BatchConfiguration.class, BatchProcessingApplication.Config.class })
public class BatchFulltextIndexUpdateTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private GenericApplicationContext context;

    @Autowired
    private AccountStateRepository repo;

    @Autowired
    private AccountStateStore store;

    @Autowired
    private SolrClient solr;

    @Autowired
    private SolrProperties solrProperties;


    private List<AccountState> states = new ArrayList<>();

    {
        Describe("Fulltext Batch Re-indexing", () -> {

            Context("given a set account statements with indexed content", () -> {

                BeforeEach(() -> {

                    for (int i=0; i<20; i++) {
                        AccountState state = new AccountState();
                        state.setName("Account Statement " + i);
                        state = repo.save(state);
                        state = store.setContent(state, new ByteArrayInputStream(String.format("Account statement %s", i).getBytes()));
                        state = repo.save(state);
                        states.add(state);
                    }

                    assertThatNameAttribute(is(nullValue()));
                });

                AfterEach(() -> {
                    for (AccountState state : states) {

                        if (store != null) {
                            store.unsetContent(state);
                        }
                        if (repo != null) {
                            repo.delete(state);
                        }
                    }
                    if (solr != null) {
                        UpdateRequest req = new UpdateRequest();
                        req.deleteByQuery("*");
                        req.setBasicAuthCredentials(solrProperties.getUser(), solrProperties.getPassword());
                        req.process(solr, null);
                        req.commit(solr, null);
                    }
                });

                Context("given a new attribute syncer that adds additional attributes", () -> {

                    BeforeEach(() -> {
                        SolrFulltextIndexServiceImpl indexer = context.getBean(SolrFulltextIndexServiceImpl.class);

                        indexer.setAttributeSyncer(new AttributeProvider<AccountState>() {

                            @Override
                            public Map<String, String> synchronize(com.example.batchprocessing.AccountState entity) {
                                Map<String,String> attrs = new HashMap<>();
                                attrs.put("name", entity.getName());
                                return attrs;
                            }
                        });
                    });

                    Context("when the re-index job is run", () -> {

                        BeforeEach(() -> {

                            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
                            JobInstance jobInstance = jobExecution.getJobInstance();
                            ExitStatus jobExitStatus = jobExecution.getExitStatus();

                            assertThat(jobInstance.getJobName(), is("reIndexContent"));
                            assertThat(jobExitStatus.getExitCode(), is("COMPLETED"));
                        });

                        It("re-index the content adding the name attribute", () -> {

                            assertThatNameAttribute(startsWith("Account Statement"));
                        });
                    });
                });
            });
        });
    }

    private void assertThatNameAttribute(Matcher matcher)
            throws SolrServerException,
            IOException {

        SolrQuery query = new SolrQuery();
        query.setQuery("_text_:Account Statement");
        query.setFields(new String[] {"id", "name"});

        QueryRequest request = new QueryRequest(query);
        request.setBasicAuthCredentials(solrProperties.getUser(), solrProperties.getPassword());

        QueryResponse response = null;
        response = request.process(solr, null);
        assertThat(response, is(not(nullValue())));

        for (SolrDocument doc : response.getResults()) {
            String name = null;
            if (doc.getFirstValue("name") != null) {
                name = format("%s", doc.getFirstValue("name"));
            }
            assertThat(name, matcher);
        }
    }

    @Test
    public void noop() {}
}
