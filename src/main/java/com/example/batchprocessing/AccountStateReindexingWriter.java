package com.example.batchprocessing;

import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AccountStateReindexingWriter implements ItemWriter<AccountState> {

    private static final Logger log = LoggerFactory.getLogger(AccountStateReindexingWriter.class);

    @Autowired
    private SolrClient solr;

    @Override
    public void write(List<? extends AccountState> items)
            throws Exception {

        for (AccountState item : items) {

            log.info("Re-indexing " + item);

            SolrInputDocument newDoc = new SolrInputDocument();

            // this requires knowledge of how Spring Content generates it's id's
            newDoc.addField("id", item.getClass().getCanonicalName() + ":" + item.getContentId());

            // new fields (loaded from tenant configuration)
            newDoc.addField("name", Collections.singletonMap("set", item.getName()));

            UpdateRequest up = new UpdateRequest();
            up.setBasicAuthCredentials("solr","SolrRocks");
            up.add(newDoc);
            up.process(solr, null);
            up.commit(solr, null);
        }
    }
}
