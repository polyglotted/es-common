package io.polyglotted.common.es.rest;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.polyglotted.common.es.ElasticClient;
import io.polyglotted.common.es.ElasticException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static io.polyglotted.common.es.ElasticException.checkState;
import static io.polyglotted.common.es.ElasticException.handleEx;
import static io.polyglotted.common.util.MapperUtil.readToMap;
import static java.net.URLEncoder.encode;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class EsRestClient implements ElasticClient {
    private static final Joiner COMMA = Joiner.on(",");
    private final RestClient restClient;
    private final Sniffer sniffer;
    private final RestHighLevelClient internalClient;

    public EsRestClient(RestClient restClient, Sniffer sniffer) { this(restClient, sniffer, new RestHighLevelClient(restClient)); }

    @Override public void close() throws Exception { if (sniffer != null) { sniffer.close(); } restClient.close(); }

    @Override public boolean indexExists(String index) { throw new UnsupportedOperationException(); }

    @Override public boolean typeExists(String index, String... types) { throw new UnsupportedOperationException(); }

    @Override public Set<String> getIndices(String alias) {
        try {
            Map<String, Object> responseObject = readToMap(performCliRequest("GET", "/" + alias + "/_aliases"));
            return ImmutableSet.copyOf(responseObject.keySet());
        } catch (Exception ioe) { throw handleEx("getIndices failed", ioe); }
    }

    @Override public String getIndexMeta(String... indices) {
        try {
            return performCliRequest("GET", "/" + COMMA.join(indices) + "/");
        } catch (Exception ioe) { throw handleEx("getIndexMeta failed", ioe); }
    }

    @Override public String getSettings(String... indices) {
        try {
            return performCliRequest("GET", "/" + COMMA.join(indices) + "/_settings");
        } catch (Exception e) { throw handleEx("getSettings failed", e); }
    }

    @Override public String getMapping(String index, String type) {
        try {
            return performCliRequest("GET", "/" + index + "/" + type + "/_mapping");
        } catch (Exception e) { throw handleEx("getSettings failed", e); }
    }

    @Override public void createIndex(CreateIndexRequest request) { throw new UnsupportedOperationException(); }

    @Override public void updateAlias(IndicesAliasesRequest request) { throw new UnsupportedOperationException(); }

    @Override public void updateSettings(UpdateSettingsRequest request) { throw new UnsupportedOperationException(); }

    @Override public void putMapping(PutMappingRequest request) { throw new UnsupportedOperationException(); }

    @Override public void forceRefresh(String... indices) {
        try {
            performCliRequest("POST", "/" + COMMA.join(indices) + "/_refresh");
        } catch (Exception ioe) { throw handleEx("forceRefresh failed", ioe); }
    }

    @Override public void dropIndex(String... indices) { throw new UnsupportedOperationException(); }

    @Override public void waitForStatus(String status) { throw new UnsupportedOperationException(); }

    @Override public Map<String, Object> clusterHealth() {
        try {
            return readToMap(performCliRequest("GET", "/_cluster/health"));
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
        try {
            return internalClient.get(request.id(encode(request.id(), "utf-8")));
        } catch (IOException ioe) { throw new ElasticException("get failed", ioe); }
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

    private String performCliRequest(String method, String endpoint) throws IOException {
        Response response = restClient.performRequest(method, endpoint);
        int statusCode = response.getStatusLine().getStatusCode();
        checkState(statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES, response.getStatusLine().getReasonPhrase());
        return EntityUtils.toString(response.getEntity());
    }
}