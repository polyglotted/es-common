package io.polyglotted.common.es.transport;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.common.es.ElasticClient;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.polyglotted.common.es.ElasticException.checkState;
import static io.polyglotted.common.es.ElasticException.handleEx;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
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

    @Override public Set<String> getIndices(String alias) {
        Set<String> indices = new HashSet<>();
        Iterator<String> indexIt = getMeta(alias).getIndices().keysIt();
        while (indexIt.hasNext()) { indices.add(indexIt.next()); }
        return indices;
    }

    @Override @SneakyThrows(IOException.class) public String getIndexMeta(String... indices) {
        MetaData indexMetaDatas = getMeta(indices);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startArray();
        ImmutableOpenMap<String, IndexMetaData> getIndices = indexMetaDatas.getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            String index = indexIt.next();
            IndexMetaData metaData = getIndices.get(index);
            builder.startObject();
            builder.startObject(index);

            builder.startObject("aliases");
            ImmutableOpenMap<String, AliasMetaData> aliases = metaData.getAliases();
            Iterator<String> aIt = aliases.keysIt();
            while (aIt.hasNext()) {
                AliasMetaData alias = aliases.get(aIt.next());
                AliasMetaData.Builder.toXContent(alias, builder, EMPTY_PARAMS);
            }
            builder.endObject();

            builder.startObject("mappings");
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.getMappings();
            Iterator<String> mIt = mappings.keysIt();
            while (mIt.hasNext()) {
                String type = mIt.next();
                builder.field(type).map(mappings.get(type).getSourceAsMap());
            }
            builder.endObject();

            builder.startObject("settings");
            Settings settings = metaData.getSettings();
            settings.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            builder.endObject();
            builder.endObject();
        }
        builder.endArray();
        return builder.string();
    }

    @Override @SneakyThrows(IOException.class) public String getSettings(String... indices) {
        MetaData indexMetaDatas = getMeta(indices);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        ImmutableOpenMap<String, IndexMetaData> getIndices = indexMetaDatas.getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            String index = indexIt.next();
            IndexMetaData metaData = getIndices.get(index);
            builder.startObject(index).startObject("settings");
            Settings settings = metaData.getSettings();
            settings.toXContent(builder, EMPTY_PARAMS);
            builder.endObject().endObject();
        }
        builder.endObject();
        return builder.string();
    }

    @Override @SneakyThrows(IOException.class) public String getMapping(String index, String type) {
        ImmutableOpenMap<String, IndexMetaData> getIndices = getMeta(index).getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            ImmutableOpenMap<String, MappingMetaData> mappings = getIndices.get(indexIt.next()).getMappings();
            Iterator<String> mIt = mappings.keysIt();
            while (mIt.hasNext()) {
                if (type.equals(mIt.next())) { return mappings.get(type).source().string(); }
            }
        }
        return null;
    }

    private MetaData getMeta(String... indices) {
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

    @Override public MultiSearchResponse multiSearch(MultiSearchRequest request) {
        try { return internalClient.multiSearch(request).actionGet(); } catch (Exception ex) { throw handleEx("multiSearch failed", ex); }
    }

    @Override public SearchResponse searchScroll(SearchScrollRequest request) {
        try { return internalClient.searchScroll(request).actionGet(); } catch (Exception ex) { throw handleEx("searchScroll failed", ex); }
    }

    @Override public ClearScrollResponse clearScroll(ClearScrollRequest request) {
        try { return internalClient.clearScroll(request).actionGet(); } catch (Exception ex) { throw handleEx("clearScroll failed", ex); }
    }

    @Override public long deleteByQuery(String index, QueryBuilder query) {
        try {
            return DeleteByQueryAction.INSTANCE.newRequestBuilder(internalClient).source(index).filter(query).get().getDeleted();
        } catch (Exception ex) { throw handleEx("clearScroll failed", ex); }
    }
}