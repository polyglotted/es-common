package io.polyglotted.common.es.rest;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.polyglotted.common.es.ElasticClient;
import io.polyglotted.common.es.ElasticException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
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
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;
import java.util.Set;

import static io.polyglotted.common.es.ElasticException.checkState;
import static io.polyglotted.common.es.ElasticException.handleEx;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class EsRestClient implements ElasticClient {
    private static final Joiner COMMA = Joiner.on(",");
    private final RestClient restClient;
    private final Sniffer sniffer;
    private final RestHighLevelClient internalClient;

    public EsRestClient(RestClient restClient, Sniffer sniffer) {
        this(restClient, sniffer, new RestHighLevelClient(restClient));
    }

    @Override public void close() throws Exception {
        if (sniffer != null) {
            sniffer.close();
        }
        restClient.close();
    }

    @Override public boolean indexExists(String index) {
        try {
            return restClient.performRequest("HEAD", "/" + index)
                    .getStatusLine().getStatusCode() == SC_OK;
        } catch (Exception ioe) {
            throw handleEx("indexExists failed", ioe);
        }
    }

    @Override public boolean typeExists(String index, String... types) {
        throw new UnsupportedOperationException();
    }

    @Override public Set<String> getIndices(String alias) {
        try {
            Map<String, Object> responseObject = deserialize(performCliRequest("GET", "/" + alias + "/_aliases"));
            return ImmutableSet.copyOf(responseObject.keySet());
        } catch (Exception ioe) {
            throw handleEx("getIndices failed", ioe);
        }
    }

    @Override public String getIndexMeta(String... indices) {
        try {
            return performCliRequest("GET", "/" + COMMA.join(indices) + "/");
        } catch (Exception ioe) {
            throw handleEx("getIndexMeta failed", ioe);
        }
    }

    @Override public String getSettings(String... indices) {
        try {
            return performCliRequest("GET", "/" + COMMA.join(indices) + "/_settings");
        } catch (Exception e) {
            throw handleEx("getSettings failed", e);
        }
    }

    @Override public String getMapping(String index, String type) {
        try {
            return performCliRequest("GET", "/" + index + "/" + type + "/_mapping");
        } catch (Exception e) {
            throw handleEx("getSettings failed", e);
        }
    }

    @Override public void openIndex(String... indices) {
        simplePost("/" + COMMA.join(indices) + "/_open", null, "openIndex");
    }

    @Override public void closeIndex(String... indices) {
        simplePost("/" + COMMA.join(indices) + "/_close", null, "closeIndex");
    }

    @Override public void createIndex(CreateIndexRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public void createIndex(String index, String resource) {
        try {
            simplePut("/" + index, resource, "createIndex");
        } catch (Exception ioe) {
            throw handleEx("createIndex failed", ioe);
        }
    }

    @Override public void updateAlias(IndicesAliasesRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public void updateSettings(UpdateSettingsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public void putMapping(PutMappingRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public void forceRefresh(String... indices) {
        try {
            performCliRequest("POST", "/" + COMMA.join(indices) + "/_refresh");
        } catch (Exception ioe) {
            throw handleEx("forceRefresh failed", ioe);
        }
    }

    @Override public void dropIndex(String... indices) {
        simpleDelete("/" + COMMA.join(indices), "dropIndex");
    }

    @Override public void waitForStatus(String status) {
        waitForStatus(status, 0);
    }

    private void waitForStatus(String status, int iter) {
        if (iter > 300) {
            throw new ElasticException("waitForStatus exceeded 300 iterations");
        }
        try {
            performCliRequest("GET", "/_cluster/health?wait_for_status=" + status);
        } catch (ConnectException | ConnectionClosedException retry) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
            waitForStatus(status, iter + 1);
        } catch (Exception ioe) {
            throw handleEx("waitForStatus failed", ioe);
        }
    }

    @Override public Map<String, Object> clusterHealth() {
        try {
            return deserialize(performCliRequest("GET", "/_cluster/health"));
        } catch (Exception ioe) {
            throw handleEx("clusterHealth failed", ioe);
        }
    }

    @Override public void buildPipeline(String id, String resource) {
        simplePut("/_ingest/pipeline/" + id, resource, "putPipeline");
    }

    @Override public boolean pipelineExists(String id) {
        return simpleGet("/_ingest/pipeline/" + id, "pipelineExists") != null;
    }

    @Override public void deletePipeline(String id) {
        simpleDelete("/_ingest/pipeline/" + id, "deletePipeline");
    }

    @Override public void putTemplate(String name, String body) {
        simplePut("/_template/" + name, body, "putTemplate");
    }

    @Override public boolean templateExists(String name) {
        return simpleGet("/_template/" + name, "templateExists") != null;
    }

    @Override public void deleteTemplate(String name) {
        simpleDelete("/_template/" + name, "deleteTemplate");
    }

    @Override public IndexResponse index(IndexRequest request) {
        try {
            return internalClient.index(request);
        } catch (IOException ioe) {
            throw new ElasticException("index failed", ioe);
        }
    }

    @Override public UpdateResponse update(UpdateRequest request) {
        try {
            return internalClient.update(request);
        } catch (IOException ioe) {
            throw new ElasticException("update failed", ioe);
        }
    }

    @Override public DeleteResponse delete(DeleteRequest request) {
        try {
            return internalClient.delete(request);
        } catch (IOException ioe) {
            throw new ElasticException("delete failed", ioe);
        }
    }

    @Override public BulkResponse bulk(BulkRequest request) {
        try {
            return internalClient.bulk(request);
        } catch (IOException ioe) {
            throw new ElasticException("bulk failed", ioe);
        }
    }

    @Override public void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener) {
        try {
            internalClient.bulkAsync(request, listener);
        } catch (Exception ioe) {
            throw new ElasticException("bulkAsync failed", ioe);
        }
    }

    @Override public GetResponse get(GetRequest request) {
        try {
            return internalClient.get(request);
        } catch (IOException ioe) {
            throw new ElasticException("get failed", ioe);
        }
    }

    @Override public MultiGetResponse multiGet(MultiGetRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public SearchResponse search(SearchRequest request) {
        try {
            return internalClient.search(request);
        } catch (IOException ioe) {
            throw new ElasticException("search failed", ioe);
        }
    }

    @Override public MultiSearchResponse multiSearch(MultiSearchRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override public SearchResponse searchScroll(SearchScrollRequest request) {
        try {
            return internalClient.searchScroll(request);
        } catch (IOException ioe) {
            throw new ElasticException("searchScroll failed", ioe);
        }
    }

    @Override public ClearScrollResponse clearScroll(ClearScrollRequest request) {
        try {
            return internalClient.clearScroll(request);
        } catch (IOException ioe) {
            throw new ElasticException("clearScroll failed", ioe);
        }
    }

    @Override public long deleteByQuery(String index, QueryBuilder query) {
        throw new UnsupportedOperationException();
    }

    @Override public String simpleGet(String endpoint, String methodName) {
        Exception throwable;
        try {
            return performCliRequest("GET", endpoint);

        } catch (ResponseException re) {
            if (re.getResponse().getStatusLine().getStatusCode() == 404) {
                return null;
            }
            throwable = re;
        } catch (Exception ioe) {
            throwable = ioe;
        }
        throw handleEx(methodName + " failed", throwable);
    }

    @Override public String simplePost(String endpoint, String body, String methodName) {
        return simpleExecute("POST", endpoint, body, methodName);
    }

    @Override public String simplePut(String endpoint, String body, String methodName) {
        return simpleExecute("PUT", endpoint, body, methodName);
    }

    @Override public void simpleDelete(String endpoint, String methodName) {
        try {
            performCliRequest("DELETE", endpoint);
        } catch (Exception ioe) {
            throw handleEx(methodName + " failed", ioe);
        }
    }

    private String simpleExecute(String method, String endpoint, String body, String methodName) {
        try {
            return performCliRequest(method, endpoint, ImmutableMap.of(),
                    new StringEntity(body == null ? "{}" : body, APPLICATION_JSON));
        } catch (Exception ioe) {
            throw handleEx(methodName + " failed", ioe);
        }
    }

    private String performCliRequest(String method, String endpoint) throws IOException {
        return performCliRequest(method, endpoint, ImmutableMap.of(), null);
    }

    private String performCliRequest(String method, String endpoint, Map<String, String> params,
                                     HttpEntity entity) throws IOException {
        Response response = restClient.performRequest(method, endpoint, params, entity);
        int statusCode = response.getStatusLine().getStatusCode();

        checkState(statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES,
                response.getStatusLine().getReasonPhrase());
        return EntityUtils.toString(response.getEntity());
    }
}