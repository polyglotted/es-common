package io.polyglotted.common.es.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.google.common.reflect.TypeToken;
import io.polyglotted.common.es.ElasticClient;
import io.polyglotted.common.es.ElasticException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.cluster.metadata.MetaData;

import java.io.IOException;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.*;
import static io.polyglotted.common.es.ElasticException.checkState;
import static io.polyglotted.common.es.ElasticException.handleEx;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class EsRestClient implements ElasticClient {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(READ_ENUMS_USING_TO_STRING, true).configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(FAIL_ON_NULL_FOR_PRIMITIVES, true).configure(ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        .configure(ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true).configure(ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)
        .setSerializationInclusion(NON_NULL).setSerializationInclusion(NON_EMPTY)
        .setVisibility(new VisibilityChecker.Std(NONE, NONE, NONE, ANY, ANY));
    @SuppressWarnings("unchecked")
    private static final Class<Map<String, Object>> MAP_CLASS = (Class<Map<String, Object>>) new TypeToken<Map<String, Object>>() {}.getRawType();
    private final RestClient restClient;
    private final Sniffer sniffer;
    private final RestHighLevelClient internalClient;

    public EsRestClient(RestClient restClient, Sniffer sniffer) { this(restClient, sniffer, new RestHighLevelClient(restClient)); }

    @Override public void close() throws Exception { if (sniffer != null) { sniffer.close(); } restClient.close(); }

    @Override public boolean indexExists(String index) { throw new UnsupportedOperationException(); }

    @Override public boolean typeExists(String index, String... types) { throw new UnsupportedOperationException(); }

    @Override public MetaData getMeta(String... indices) { throw new UnsupportedOperationException(); }

    @Override public void createIndex(CreateIndexRequest request) { throw new UnsupportedOperationException(); }

    @Override public void updateAlias(IndicesAliasesRequest request) { throw new UnsupportedOperationException(); }

    @Override public void updateSettings(UpdateSettingsRequest request) { throw new UnsupportedOperationException(); }

    @Override public void putMapping(PutMappingRequest request) { throw new UnsupportedOperationException(); }

    @Override public void forceRefresh(String... indices) { throw new UnsupportedOperationException(); }

    @Override public void dropIndex(String... indices) { throw new UnsupportedOperationException(); }

    @Override public void waitForStatus(String status) { throw new UnsupportedOperationException(); }

    @Override public Map<String, Object> clusterHealth() {
        try {
            Response response = restClient.performRequest("GET", "/_cluster/health");
            checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK, response.getStatusLine().getReasonPhrase());
            return MAPPER.readValue(EntityUtils.toString(response.getEntity()), MAP_CLASS);
        } catch (Exception ioe) { throw handleEx("clusterHealth failed", ioe); }
    }

    @Override public void buildPipeline(String id, String resource) { throw new UnsupportedOperationException(); }

    @Override public boolean pipelineExists(String id) { throw new UnsupportedOperationException(); }

    @Override public IndexResponse index(IndexRequest request) {
        try { return internalClient.index(request); } catch (IOException ioe) { throw new ElasticException("index failed", ioe); }
    }

    @Override public UpdateResponse update(UpdateRequest request) {
        try { return internalClient.update(request); } catch (IOException ioe) { throw new ElasticException("update failed", ioe); }
    }

    @Override public DeleteResponse delete(DeleteRequest request) {
        try { return internalClient.delete(request); } catch (IOException ioe) { throw new ElasticException("delete failed", ioe); }
    }

    @Override public BulkResponse bulk(BulkRequest request) {
        try { return internalClient.bulk(request); } catch (IOException ioe) { throw new ElasticException("bulk failed", ioe); }
    }

    @Override public void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener) {
        try { internalClient.bulkAsync(request, listener); } catch (Exception ioe) { throw new ElasticException("bulkAsync failed", ioe); }
    }

    @Override public GetResponse get(GetRequest request) {
        try { return internalClient.get(request); } catch (IOException ioe) { throw new ElasticException("get failed", ioe); }
    }

    @Override public MultiGetResponse multiGet(MultiGetRequest request) { throw new UnsupportedOperationException(); }

    @Override public SearchResponse search(SearchRequest request) {
        try { return internalClient.search(request); } catch (IOException ioe) { throw new ElasticException("search failed", ioe); }
    }

    @Override public SearchResponse searchScroll(SearchScrollRequest request) {
        try { return internalClient.searchScroll(request); } catch (IOException ioe) { throw new ElasticException("searchScroll failed", ioe); }
    }

    @Override public ClearScrollResponse clearScroll(ClearScrollRequest request) {
        try { return internalClient.clearScroll(request); } catch (IOException ioe) { throw new ElasticException("clearScroll failed", ioe); }
    }
}