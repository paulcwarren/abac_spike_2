package com.example.batchprocessing;

import org.springframework.content.commons.repository.ContentStore;
import org.springframework.content.commons.search.Searchable;
import org.springframework.stereotype.Component;

@Component
public interface AccountStateStore extends ContentStore<AccountState, String>, Searchable<String> {
}
