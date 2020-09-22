package com.example.batchprocessing;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AccountStateReindexingWriter implements ItemWriter<AccountState> {

    private static final Logger log = LoggerFactory.getLogger(AccountStateReindexingWriter.class);

    @Autowired
    private AccountStateRepository repo;

    @Autowired
    private AccountStateStore store;

    @Override
    public void write(List<? extends AccountState> items)
            throws Exception {

        for (AccountState item : items) {

            log.info("Re-indexing " + item);

            InputStream in = store.getContent(item);
            byte[] content = IOUtils.toByteArray(in);

            item = store.setContent(item, new ByteArrayInputStream(content));
            repo.save(item);
        }
    }
}
