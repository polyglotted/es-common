package io.polyglotted.common.es.rest;

import io.polyglotted.common.es.ElasticClient;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

@RequiredArgsConstructor
public class EsRestClient implements ElasticClient {
    private final RestHighLevelClient internalClient;

    @Override public SearchResponse search(SearchRequest request) {
        try { return internalClient.search(request); } catch (IOException ioe) { throw new RuntimeException(ioe); }
    }
}