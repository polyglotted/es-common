package io.polyglotted.common.es.transport;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.common.es.ElasticClient;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static io.polyglotted.common.es.ElasticException.checkState;
import static io.polyglotted.common.es.ElasticException.handleEx;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@RequiredArgsConstructor
public class EsTransportClient implements ElasticClient {
    private final Client internalClient;

    @Override public void close() { internalClient.close(); }

    @Override public boolean indexExists(String index) {
        try {
            return internalClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
        } catch (Exception ex) { throw handleEx("indexExists failed", ex); }
    }

    @Override public boolean typeExists(String index, String... types) {
        try {
            return internalClient.admin().indices().typesExists(new TypesExistsRequest(new String[]{index}, types)).actionGet().isExists();
        } catch (Exception ex) { throw handleEx("typeExists failed", ex); }
    }

    @Override public MetaData getMeta(String... indices) {
        try {
            return internalClient.admin().cluster().prepareState().setIndices(indices).execute().actionGet().getState().metaData();
        } catch (Exception ex) { throw handleEx("getMeta failed", ex); }
    }

    @Override public void createIndex(CreateIndexRequest request) {
        try {
            checkState(internalClient.admin().indices().create(request).actionGet().isAcknowledged(), "unable to create index for " + request.index());
        } catch (Exception ex) { throw handleEx("createIndex failed", ex); }
    }

    @Override public void updateAlias(IndicesAliasesRequest request) {
        try {
            checkState(internalClient.admin().indices().aliases(request).actionGet().isAcknowledged(), "unable to update aliases");
        } catch (Exception ex) { throw handleEx("updateAlias failed", ex); }
    }

    @Override public void updateSettings(UpdateSettingsRequest request) {
        try {
            checkState(internalClient.admin().indices().updateSettings(request).actionGet().isAcknowledged(), "unable to update settings");
        } catch (Exception ex) { throw handleEx("updateSettings failed", ex); }
    }

    @Override public void putMapping(PutMappingRequest request) {
        try {
            checkState(internalClient.admin().indices().putMapping(request).actionGet().isAcknowledged(), "could not put mapping for " + request.type());
        } catch (Exception ex) { throw handleEx("putMapping failed", ex); }
    }

    @Override public void forceRefresh(String... indices) {
        try {
            internalClient.admin().indices().refresh(refreshRequest(indices)).actionGet();
        } catch (Exception ex) { throw handleEx("refresh failed", ex); }
    }

    @Override public void dropIndex(String... indices) {
        try {
            checkState(internalClient.admin().indices().delete(new DeleteIndexRequest(indices).indicesOptions(lenientExpandOpen()))
                .actionGet().isAcknowledged(), "Could not clear one or more index " + Arrays.toString(indices));
        } catch (Exception ex) { throw handleEx("dropIndex failed", ex); }
    }

    @Override public void waitForStatus(String status) {
        try {
            ClusterHealthResponse clusterHealth = internalClient.admin().cluster().prepareHealth().setWaitForNoRelocatingShards(true)
                .setWaitForStatus(ClusterHealthStatus.fromString(status)).execute().actionGet();
            checkState(clusterHealth.getStatus() != ClusterHealthStatus.RED, "cluster has errors");
        } catch (Exception ex) { throw handleEx("waitForStatus failed", ex); }
    }

    @Override public Map<String, Object> clusterHealth() {
        try {
            ClusterHealthResponse health = internalClient.admin().cluster().health(new ClusterHealthRequest()).actionGet();
            return ImmutableMap.<String, Object>builder()
                .put("cluster_name", health.getClusterName())
                .put("status", health.getStatus().name().toLowerCase(Locale.ROOT))
                .put("timed_out", health.isTimedOut())
                .put("number_of_nodes", health.getNumberOfNodes())
                .put("number_of_data_nodes", health.getNumberOfDataNodes())
                .put("active_primary_shards", health.getActivePrimaryShards())
                .put("active_shards", health.getActiveShards())
                .put("relocating_shards", health.getRelocatingShards())
                .put("initializing_shards", health.getInitializingShards())
                .put("unassigned_shards", health.getUnassignedShards())
                .put("delayed_unassigned_shards", health.getDelayedUnassignedShards())
                .put("number_of_pending_tasks", health.getNumberOfPendingTasks())
                .put("number_of_in_flight_fetch", health.getNumberOfInFlightFetch())
                .put("task_max_waiting_in_queue_millis", health.getTaskMaxWaitingTime().millis() == 0 ? "-" : health.getTaskMaxWaitingTime().getStringRep())
                .put("active_shards_percent_as_number", String.format(Locale.ROOT, "%1.1f%%", health.getActiveShardsPercent()))
                .build();
        } catch (Exception ex) { throw handleEx("clusterHealth failed", ex); }
    }

    @Override public void buildPipeline(String id, String json) {
        try {
            checkState(internalClient.admin().cluster().preparePutPipeline(id, new BytesArray(json), JSON)
                .execute().actionGet().isAcknowledged(), "unable to build pipeline");
        } catch (Exception ex) { throw handleEx("buildPipeline failed", ex); }
    }

    @Override public boolean pipelineExists(String id) {
        try {
            return internalClient.admin().cluster().prepareGetPipeline(id).execute().actionGet().isFound();
        } catch (Exception ex) { throw handleEx("pipelineExists failed", ex); }
    }

    @Override public IndexResponse index(IndexRequest request) {
        try { return internalClient.index(request).actionGet(); } catch (Exception ex) { throw handleEx("index failed", ex); }
    }

    @Override public UpdateResponse update(UpdateRequest request) {
        try { return internalClient.update(request).actionGet(); } catch (Exception ex) { throw handleEx("update failed", ex); }
    }

    @Override public DeleteResponse delete(DeleteRequest request) {
        try { return internalClient.delete(request).actionGet(); } catch (Exception ex) { throw handleEx("delete failed", ex); }
    }

    @Override public BulkResponse bulk(BulkRequest request) {
        try { return internalClient.bulk(request).actionGet(); } catch (Exception ex) { throw handleEx("bulk failed", ex); }
    }

    @Override public void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener) {
        try { internalClient.bulk(request, listener); } catch (Exception ex) { throw handleEx("bulkAsync failed", ex); }
    }

    @Override public GetResponse get(GetRequest request) {
        try { return internalClient.get(request).actionGet(); } catch (Exception ex) { throw handleEx("get failed", ex); }
    }

    @Override public MultiGetResponse multiGet(MultiGetRequest request) {
        try { return internalClient.multiGet(request).actionGet(); } catch (Exception ex) { throw handleEx("multiGet failed", ex); }
    }

    @Override public SearchResponse search(SearchRequest request) {
        try { return internalClient.search(request).actionGet(); } catch (Exception ex) { throw handleEx("search failed", ex); }
    }

    @Override public SearchResponse searchScroll(SearchScrollRequest request) {
        try { return internalClient.searchScroll(request).actionGet(); } catch (Exception ex) { throw handleEx("searchScroll failed", ex); }
    }

    @Override public ClearScrollResponse clearScroll(ClearScrollRequest request) {
        try { return internalClient.clearScroll(request).actionGet(); } catch (Exception ex) { throw handleEx("clearScroll failed", ex); }
    }
}