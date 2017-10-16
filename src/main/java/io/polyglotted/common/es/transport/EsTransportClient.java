package io.polyglotted.common.es.transport;

import io.polyglotted.common.es.ElasticClient;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

@RequiredArgsConstructor
public class EsTransportClient implements ElasticClient {
    private final Client internalClient;

    @Override public SearchResponse search(SearchRequest request) { return internalClient.search(request).actionGet(); }
}