package io.polyglotted.common.es;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

public interface ElasticClient {

    SearchResponse search(SearchRequest request);
}